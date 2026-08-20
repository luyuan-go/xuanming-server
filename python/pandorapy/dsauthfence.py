"""DS 回调授权协议的进程级激活栅栏 —— 对应 Go 侧 `pkg/dsauthfence` 的核心三件:

    pkg/dsauthfence/fence.go     (765 行)  策略常量 / RequiredState / Capability / Holder
    pkg/dsauthfence/etcd.go      (169 行)  etcd Backend + capability 租约
    pkg/dsauthfence/security.go  (257 行)  etcd 客户端的生产安全姿态(mTLS / 最小权限证明)

`activate.go` 的激活工具**不在本模块**,由 `pandorapy/dsauthfence_activate.py` 承接;
本模块保证它需要的公开面(key 模板、策略表、RequiredState 解析、激活策略校验、
`expected_services_hash`)完整且稳定。

★ 本模块保护的不变量(CLAUDE.md §9 不变量 22 / §16.4)

    Redis 的每实例 `required_writer_epoch` 负责数据面的最终拒绝;本模块负责**控制面**:
    进程启动时线性读 etcd 全局 required epoch,注册带租约的 capability,并持续 watch。
    初读失败、required 回退/删除、未来 epoch、租约丢失 —— 任何一条都会置位 `lost`,
    调用方**必须立即停止受保护写并退出**。

★ 两栈共存的硬约束(strangler 模式)

    Go 与 Python 读**同一份** etcd。所以下面三样东西是**逐字节契约**,不是风格选择:

      ① key 模板    `/pandora/ds-auth/` + `required-writer-epoch` / `capabilities/<svc>/<uid>`
                     / `activation-lock`。前缀写错 = 两栈各写各的 = 双 writer 脑裂。
      ② required 值 `"1"` / `"2@ds-auth-v2-pod-uid-write-invariant-v1"`
                     / `"2@ds-auth-v2-hub-successor-lease-v1"`。capability 注册的 CAS
                     直接比这串字节;差一个字符 = 永远注册不上(或更糟:注册到旧策略上)。
      ③ capability  JSON 字段名与 Go struct tag 一致,Go 的激活审计要 Unmarshal 它。

★ aetcd 没有自动 KeepAlive —— 这是整条移植里最容易写出双写者的一处

    Go 的 `clientv3.KeepAlive` 是一条**流**:租约没了流就断,`monitorLease` 立刻收到。
    aetcd 只有一次性的 `lease.refresh()`,而且 etcd 对**已经不存在**的 lease 的
    keepalive 应答是「正常返回、TTL=0」(见 `pandorapy/etcdlease.py` 的实测记录)。
    所以续租节奏、失败判定、自 fencing 全部由本模块负责,规则与 `etcdleader` /
    `snowflake_etcd` 完全一致:

      - 只把**当前 lease 的成功响应**视为续租成功(已发送 / 超时 / UNKNOWN 都不算);
      - 用 `time.monotonic()` 维护一个**比服务端 lease 到期更早**的本地安全截止线
        (墙钟回拨会让安全窗直接失效,所以禁止 `time.time()`);
      - 越过安全线立即置位 `lost` 并**自 fencing**(`holding()` 永久返回 False),
        不等"可能永不到来"的响应,也不因迟到的成功响应复活。

    自 fencing 是**单调终态**:同一个租约一旦被判失去,不可能再被任何后续证据救回来。
    否则会出现"本副本以为还持有、实际 capability 已被别人接管"的静默双写者。

★ 生产安全姿态:aetcd 无法建立 mTLS,因此走 fail-closed(见 `new_etcd_client`)

    `aetcd.Client.connect()` 无条件使用 `rpc.insecure_channel`(1.x 实测,见本仓
    `etcdleader` / `snowflake_etcd` 也都是明文连接)。本模块**不**假装安全:配置里
    一旦出现任何 `PANDORA_DS_AUTH_ETCD_*` 安全项(即 `ClientSecurity.enabled()`),
    先做与 Go 完全一致的**全量配置校验**,然后**明确报错拒绝连接**,而不是悄悄退回明文。
    静默明文连接会让"以为有 mTLS + 最小权限"的部署在毫无信号的情况下裸奔。
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import json
import os
import re
import ssl
import time
import urllib.parse
from collections.abc import AsyncIterator, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Protocol

import aetcd
import aetcd.rtypes as _rtypes

from pandorapy import errcode
from pandorapy import log as plog
from pandorapy import safego
from pandorapy.etcdlease import LeaseGoneError, refresh_or_raise

__all__ = [
    "ACTIVATION_LOCK_SUFFIX",
    "CAPABILITY_FEATURE_PATTERN",
    "DEFAULT_DIAL_TIMEOUT_SEC",
    "DEFAULT_LEASE_TTL_SEC",
    "DEFAULT_PREFIX",
    "DIGEST_PATTERN",
    "ENV_ETCD_CA_FILE",
    "ENV_ETCD_CERT_FILE",
    "ENV_ETCD_CLIENT_IDENTITY",
    "ENV_ETCD_FORBIDDEN_READ_PREFIX",
    "ENV_ETCD_IDENTITY_REVISION",
    "ENV_ETCD_KEY_FILE",
    "ENV_ETCD_PASSWORD_FILE",
    "ENV_ETCD_REQUIRE_AUTH",
    "ENV_ETCD_REQUIRE_MTLS",
    "ENV_ETCD_SERVER_NAME",
    "ENV_ETCD_USERNAME_FILE",
    "ENV_IMAGE_DIGEST",
    "ENV_POD_UID",
    "ERR_TOPOLOGY_CHANGE_LOCK_PROVIDER_UNAVAILABLE",
    "ETCD_IDENTITY_REVISION_PATTERN",
    "LOST_REASON_LEASE_KEEPALIVE_ENDED",
    "LOST_REASON_REQUIRED_ADVANCED",
    "LOST_REASON_REQUIRED_DELETED",
    "LOST_REASON_REQUIRED_REGRESSED",
    "LOST_REASON_REQUIRED_WATCH_CLOSED",
    "LOST_REASON_REQUIRED_WATCH_ERROR",
    "PROTOCOL_EPOCH_V2",
    "REQUIRED_POLICY_GENERATION_V1",
    "REQUIRED_POLICY_GENERATION_V2",
    "REQUIRED_POLICY_GENERATION_V3",
    "REQUIRED_POLICY_V2",
    "REQUIRED_POLICY_V2_FEATURES",
    "REQUIRED_POLICY_V3",
    "REQUIRED_POLICY_V3_FEATURES",
    "REQUIRED_VALUE_V2",
    "REQUIRED_VALUE_V3",
    "Backend",
    "Capability",
    "CapabilityFencedError",
    "CapabilityLostError",
    "CapabilityRead",
    "ClientSecurity",
    "Config",
    "FenceError",
    "Holder",
    "Lease",
    "RequiredEvent",
    "RequiredRead",
    "RequiredState",
    "RuntimeConfig",
    "SecureEtcdUnsupportedError",
    "TLSMaterial",
    "TopologyChangeLockProviderUnavailableError",
    "acquire",
    "acquire_runtime",
    "activation_lock_key",
    "capability_key",
    "capability_prefix",
    "clean_prefix",
    "client_security_from_env",
    "dial_secure_etcd_client",
    "equal_feature_set",
    "expected_services_hash",
    "load_tls_material",
    "new_etcd_client",
    "parse_epoch",
    "parse_required_state",
    "required_features_for_policy_generation",
    "required_key",
    "required_policy_id_for_generation",
    "required_value_for_epoch",
    "required_value_for_policy_generation",
    "required_writer_epoch_for_policy_generation",
    "start",
    "validate_activation_policy",
    "validate_activation_policy_generation",
    "validate_client_security",
    "validate_features",
    "validate_required_policy_for_capability",
    "validate_same_pod_takeover",
]

# ── 整数边界 ────────────────────────────────────────────────────────────────
#
# Go 的 uint32 / int64 会**回绕**,Python 整数无限精度 —— 于是同一份坏数据在两栈
# 表现不同:Go 截断成一个"看起来正常"的小数,Python 原样带着一个天文数字继续跑。
# 跨栈读同一份 etcd 时这就是分叉,所以外部来的数值必须在边界处显式判。
_MAX_UINT32 = 0xFFFF_FFFF
_MAX_INT64 = 0x7FFF_FFFF_FFFF_FFFF

# ★ 正则一律 `\A...\Z`。Python 的 `$` 会匹配**结尾换行**(`re.match(r"^a$", "a\n")`
# 成功),Go 的 `$` 在非多行模式下不会 —— 于是 "sha256:<64hex>\n" 这种带尾换行的
# 环境变量在 Python 侧通过、Go 侧拒绝,正是校验器最不该有的分叉。
CAPABILITY_FEATURE_PATTERN = re.compile(r"\A[a-z][a-z0-9-]{2,63}\Z")
DIGEST_PATTERN = re.compile(r"\Asha256:[0-9a-f]{64}\Z")
ETCD_IDENTITY_REVISION_PATTERN = re.compile(r"\Ar[1-9][0-9]*\Z")

# ErrTopologyChangeLockProviderUnavailable 是**发布阻断**,不是可重试的 etcd 故障:
# 本仓不存在任何 provider API / 信任根能在「预检 → CAS」整个窗口内阻止 Redis
# failover / reshard / 原子槽位迁移。文案与 Go 逐字节一致。
ERR_TOPOLOGY_CHANGE_LOCK_PROVIDER_UNAVAILABLE = (
    "dsauthfence: authoritative Redis topology-change lock provider is not wired; "
    "target epoch CAS is disabled"
)

# ProtocolEpochV2 是 Redis active/pending 完整凭据协议版本。
PROTOCOL_EPOCH_V2 = 2
# RequiredPolicyV2 是 etcd required **值的一部分**,不是部署配置。旧的 epoch-2 二进制
# 只认得裸数字 "2",看到 REQUIRED_VALUE_V2 会 fail-closed —— 这正是回滚栅栏。
REQUIRED_POLICY_V2 = "ds-auth-v2-pod-uid-write-invariant-v1"
REQUIRED_VALUE_V2 = "2@" + REQUIRED_POLICY_V2
# RequiredPolicyV3 推进不可变控制面策略,而 Redis / 数据面 writer 协议仍是 epoch 2。
# 新起一个 raw value(而不是改 V2)才是**持久的回滚栅栏**:V2 二进制解析不了它,
# 因此不可能重新拿到 writer capability。
REQUIRED_POLICY_V3 = "ds-auth-v2-hub-successor-lease-v1"
REQUIRED_VALUE_V3 = "2@" + REQUIRED_POLICY_V3
REQUIRED_POLICY_GENERATION_V1 = 1
REQUIRED_POLICY_GENERATION_V2 = 2
REQUIRED_POLICY_GENERATION_V3 = 3

# DEFAULT_PREFIX 是全局 required 与 capability key 的根前缀。★ 逐字符照抄 Go。
DEFAULT_PREFIX = "/pandora/ds-auth/"
# capability 租约 TTL(秒)。
DEFAULT_LEASE_TTL_SEC = 15
# etcd 启动期操作超时(秒)。Go 侧是 5 * time.Second。
DEFAULT_DIAL_TIMEOUT_SEC = 5.0

# EnvPodUID / EnvImageDigest 必须由 K8s Downward API 注入,并由准入策略与 Pod 实体绑定。
ENV_POD_UID = "PANDORA_POD_UID"
ENV_IMAGE_DIGEST = "PANDORA_IMAGE_DIGEST"

# key 后缀(与 Go 的 requiredKey / capabilityPrefix / activationLockKey 同源)。
_REQUIRED_KEY_SUFFIX = "required-writer-epoch"
_CAPABILITY_PREFIX_SUFFIX = "capabilities/"
ACTIVATION_LOCK_SUFFIX = "activation-lock"

# ── 续租节奏与本地安全余量 ──────────────────────────────────────────────────
#
# Go 的 clientv3.KeepAlive 内部按 TTL/3 续;本地安全余量同样取 TTL/3,于是本地
# 可持有窗口 = TTL*2/3(TTL=15s → 10s)。含义:**连续丢一次续租就进入放弃流程**。
#
# 为什么本地截止必须早于服务端 lease 到期:到期判定在 etcd 服务端,本地时钟、网络
# 往返、GIL 抖动都会让"我以为还有 1s"实际已经过期。§9.22 要求
# `旧持有者最晚停止时间 < 新持有者最早开始时间`,这个余量就是该不等式的兑现。
_KEEPALIVE_DIVISOR = 3
_SAFETY_MARGIN_DIVISOR = 3

# 失效原因(LostReason)。五个触发分支在最终日志里此前完全同形,事故取证
# (2026-07-29 ds_allocator 保护性退出)无法继续细分"是失租还是 watch 断",必须区分。
# ★ 常量取值与 Go 逐字节一致 —— 它们会进日志与审计,漂移即查询落空。
LOST_REASON_LEASE_KEEPALIVE_ENDED = "lease_keepalive_ended"
LOST_REASON_REQUIRED_WATCH_ERROR = "required_watch_error"
LOST_REASON_REQUIRED_DELETED = "required_deleted_or_empty"
LOST_REASON_REQUIRED_REGRESSED = "required_regressed_or_invalid"
LOST_REASON_REQUIRED_ADVANCED = "required_advanced"
LOST_REASON_REQUIRED_WATCH_CLOSED = "required_watch_closed"

# requiredPolicyV2Features 是**完整的**生产 writer 集合。保持精确:新增服务或
# feature 必须起新的 policy ID,否则旧二进制可能悄悄以既有策略重新入场。
REQUIRED_POLICY_V2_FEATURES: dict[str, tuple[str, ...]] = {
    "login": (),
    "player_locator": (),
    "ds_allocator": (
        "battle-release-expected-tuple-v1",
        "battle-storage-pod-uid-write-invariant-v1",
    ),
    "hub_allocator": (
        "hub-reservation-ledger-v1",
        "hub-heartbeat-capacity-v1",
        "hub-owner-cleanup-v1",
        "hub-physical-eviction-v1",
    ),
    "battle_result": ("battle-terminal-outbox-v1",),
}

# requiredPolicyV3Features 是一份**新的不可变策略集**。不要把新 feature 加进 V2:
# 那会让同一个 etcd 值对不同二进制含义不同,回滚窗口重新打开。
REQUIRED_POLICY_V3_FEATURES: dict[str, tuple[str, ...]] = {
    "login": (),
    "player_locator": (),
    "ds_allocator": (
        "battle-release-expected-tuple-v1",
        "battle-storage-pod-uid-write-invariant-v1",
    ),
    "hub_allocator": (
        "hub-reservation-ledger-v1",
        "hub-heartbeat-capacity-v1",
        "hub-owner-cleanup-v1",
        "hub-physical-eviction-v1",
        "hub-successor-lease-v1",
    ),
    "battle_result": ("battle-terminal-outbox-v1",),
}

# etcd 客户端安全环境变量(与 Go security.go 逐字节一致)。
ENV_ETCD_REQUIRE_MTLS = "PANDORA_DS_AUTH_ETCD_REQUIRE_MTLS"
ENV_ETCD_CA_FILE = "PANDORA_DS_AUTH_ETCD_CA_FILE"
ENV_ETCD_CERT_FILE = "PANDORA_DS_AUTH_ETCD_CERT_FILE"
ENV_ETCD_KEY_FILE = "PANDORA_DS_AUTH_ETCD_KEY_FILE"
ENV_ETCD_SERVER_NAME = "PANDORA_DS_AUTH_ETCD_SERVER_NAME"
ENV_ETCD_CLIENT_IDENTITY = "PANDORA_DS_AUTH_ETCD_CLIENT_IDENTITY"
ENV_ETCD_IDENTITY_REVISION = "PANDORA_DS_AUTH_ETCD_IDENTITY_REVISION"
ENV_ETCD_USERNAME_FILE = "PANDORA_DS_AUTH_ETCD_USERNAME_FILE"
ENV_ETCD_PASSWORD_FILE = "PANDORA_DS_AUTH_ETCD_PASSWORD_FILE"
ENV_ETCD_REQUIRE_AUTH = "PANDORA_DS_AUTH_ETCD_REQUIRE_AUTH"
ENV_ETCD_FORBIDDEN_READ_PREFIX = "PANDORA_DS_AUTH_ETCD_FORBIDDEN_READ_PREFIX"

# safego 任务名。★ 必须是**静态**字面量:safego 把 name 当 Prometheus label
# (`pandora_safego_panic_recovered_total{name}`),拼进 service / pod uid 就是
# 高基数标签(§12 明令禁止 player_id 那类值当 label)。
_TASK_LEASE_KEEPALIVE = "dsauthfence-lease-keepalive"
_TASK_MONITOR_LEASE = "dsauthfence-monitor-lease"
_TASK_MONITOR_REQUIRED = "dsauthfence-monitor-required"


# ── 异常 ────────────────────────────────────────────────────────────────────


class FenceError(errcode.PandoraError):
    """栅栏类错误的基类。

    ★ 证据一律走 `PandoraError` 的**声明式 `__slots__`**,禁止 `setattr` ——
    后者能写进去(Exception 自带 `__dict__`)但拼错一个字母不会报错,
    读的那侧只会永远拿到默认值(见 errcode.PandoraError 的注释)。
    """

    __slots__ = ()


class CapabilityFencedError(FenceError):
    """capability 注册被 fencing 拦下:重复 key / 激活锁在持 / required 已变。

    `capability_key` 是取证必需 —— 五条 fail-closed 路径在日志里长得一样,
    没有 key 就分不清"是自己那把锁"还是"别人的"。
    """

    __slots__ = ("capability_key",)

    def __init__(self, key: str) -> None:
        super().__init__(
            errcode.ErrInvalidState,
            "capability registration fenced by duplicate key, activation lock, "
            "or required epoch change: %s",
            key,
        )
        self.capability_key = key


class CapabilityLostError(FenceError):
    """capability 已失效。**调用方必须立即停止受保护写并退出**。

    `lost_reason` 是 LOST_REASON_* 之一;失去它就退回到 2026-07-29 那次事故的处境:
    五个触发分支同形,分不出是失租还是 watch 断。
    """

    __slots__ = ("lost_reason",)

    def __init__(self, reason: str) -> None:
        super().__init__(
            errcode.ErrInvalidState, "dsauthfence: capability lost: %s", reason
        )
        self.lost_reason = reason


class TopologyChangeLockProviderUnavailableError(FenceError):
    """对应 Go 的 ErrTopologyChangeLockProviderUnavailable —— 发布阻断,不可重试。"""

    __slots__ = ()

    def __init__(self) -> None:
        super().__init__(
            errcode.ErrInvalidState, ERR_TOPOLOGY_CHANGE_LOCK_PROVIDER_UNAVAILABLE
        )


class SecureEtcdUnsupportedError(FenceError):
    """配置要求安全 etcd,但 aetcd 无法建立 mTLS —— fail-closed,不退回明文。"""

    __slots__ = ()

    def __init__(self, detail: str) -> None:
        super().__init__(errcode.ErrUnavailable, "%s", detail)


def _invalid(msg: str, *args: object) -> FenceError:
    return FenceError(errcode.ErrInvalidArg, msg, *args)


def _unavailable(msg: str, *args: object) -> FenceError:
    return FenceError(errcode.ErrUnavailable, msg, *args)


def _fail_closed(msg: str, *args: object) -> FenceError:
    return FenceError(errcode.ErrInvalidState, msg, *args)


# ── key 模板(★ 逐字符照抄 Go;写错即两栈脑裂)────────────────────────────


def clean_prefix(prefix: str) -> str:
    """`strings.TrimSuffix(prefix, "/") + "/"` 的逐字对应。"""
    return prefix.removesuffix("/") + "/"


def required_key(prefix: str) -> str:
    return clean_prefix(prefix) + _REQUIRED_KEY_SUFFIX


def capability_prefix(prefix: str) -> str:
    return clean_prefix(prefix) + _CAPABILITY_PREFIX_SUFFIX


def activation_lock_key(prefix: str) -> str:
    return clean_prefix(prefix) + ACTIVATION_LOCK_SUFFIX


def capability_key(prefix: str, service: str, uid: str) -> str:
    return capability_prefix(prefix) + service + "/" + uid


# ── 值解析 ──────────────────────────────────────────────────────────────────


def parse_epoch(raw: bytes | str) -> int:
    """只接受规范十进制正整数,避免 "02" 等多种字节表示破坏 CAS。

    ★ Go 的 `strconv.ParseUint(s, 10, 32)` 会拒绝 "+2" / " 2" / "2\\n" / 超 uint32;
    Python 的 `int()` 会接受 "+2" 和带空白的串,所以这里必须**显式**逐条判。
    """
    s = raw.decode("utf-8", "replace") if isinstance(raw, bytes) else raw
    if s == "" or s[0] == "0" or s.strip() != s:
        raise _invalid("invalid epoch %r", s)
    if not s.isdigit() or not s.isascii():
        # isdigit() 对全角数字 "２" 返回 True,int() 也认;Go 不认。
        raise _invalid("invalid epoch %r", s)
    value = int(s)
    if value == 0 or value > _MAX_UINT32:
        raise _invalid("invalid epoch %r", s)
    return value


@dataclass(frozen=True, slots=True)
class RequiredState:
    """规范化、带版本的 etcd fencing 值。

    `raw_value` 在 capability 注册事务里被**逐字节**比较 —— 它不是展示字段。
    """

    epoch: int = 0
    policy_generation: int = 0
    policy_id: str = ""
    raw_value: str = ""


def parse_required_state(raw: bytes | str) -> RequiredState:
    """解析 required 值。

    ★ **刻意不接受裸 "2"** —— 这条字节级不兼容正是旧 epoch-2 二进制的回滚栅栏。
    """
    s = raw.decode("utf-8", "replace") if isinstance(raw, bytes) else raw
    if s == "1":
        return RequiredState(
            epoch=1, policy_generation=REQUIRED_POLICY_GENERATION_V1, raw_value=s
        )
    if s == REQUIRED_VALUE_V2:
        return RequiredState(
            epoch=PROTOCOL_EPOCH_V2,
            policy_generation=REQUIRED_POLICY_GENERATION_V2,
            policy_id=REQUIRED_POLICY_V2,
            raw_value=s,
        )
    if s == REQUIRED_VALUE_V3:
        return RequiredState(
            epoch=PROTOCOL_EPOCH_V2,
            policy_generation=REQUIRED_POLICY_GENERATION_V3,
            policy_id=REQUIRED_POLICY_V3,
            raw_value=s,
        )
    raise _fail_closed("invalid or unsupported required writer policy %r", s)


def required_value_for_policy_generation(generation: int) -> str:
    """控制面策略代 → 不可变 etcd raw value。V2 与 V3 **刻意共享**数据面 epoch 2。"""
    if generation == REQUIRED_POLICY_GENERATION_V1:
        return "1"
    if generation == REQUIRED_POLICY_GENERATION_V2:
        return REQUIRED_VALUE_V2
    if generation == REQUIRED_POLICY_GENERATION_V3:
        return REQUIRED_VALUE_V3
    raise _invalid("unsupported required policy generation %d", generation)


def required_policy_id_for_generation(generation: int) -> str:
    if generation == REQUIRED_POLICY_GENERATION_V1:
        return ""
    if generation == REQUIRED_POLICY_GENERATION_V2:
        return REQUIRED_POLICY_V2
    if generation == REQUIRED_POLICY_GENERATION_V3:
        return REQUIRED_POLICY_V3
    raise _invalid("unsupported required policy generation %d", generation)


def required_writer_epoch_for_policy_generation(generation: int) -> int:
    if generation == REQUIRED_POLICY_GENERATION_V1:
        return 1
    if generation in (REQUIRED_POLICY_GENERATION_V2, REQUIRED_POLICY_GENERATION_V3):
        return PROTOCOL_EPOCH_V2
    raise _invalid("unsupported required policy generation %d", generation)


def required_features_for_policy_generation(generation: int) -> dict[str, tuple[str, ...]]:
    if generation == REQUIRED_POLICY_GENERATION_V2:
        return REQUIRED_POLICY_V2_FEATURES
    if generation == REQUIRED_POLICY_GENERATION_V3:
        return REQUIRED_POLICY_V3_FEATURES
    raise _invalid("unsupported required policy generation %d", generation)


def required_value_for_epoch(epoch: int) -> str:
    """受支持迁移的**唯一**规范值。刻意不可运行期配置。"""
    if epoch == 1:
        return "1"
    if epoch == PROTOCOL_EPOCH_V2:
        return REQUIRED_VALUE_V2
    raise _invalid("unsupported required writer epoch %d", epoch)


# ── Capability 记录(写进 etcd lease key 的审计记录)────────────────────────


def _json_str(obj: Mapping[str, object], key: str) -> str:
    value = obj.get(key)
    if value is None:
        return ""
    if not isinstance(value, str):
        raise _fail_closed("capability field %s is not a string", key)
    return value


def _json_uint32(obj: Mapping[str, object], key: str) -> int:
    value = obj.get(key)
    if value is None:
        return 0
    # bool 是 int 的子类:`True` 会被当成 1 悄悄通过,而 Go 的 json 解码会报错。
    if isinstance(value, bool) or not isinstance(value, int):
        raise _fail_closed("capability field %s is not an integer", key)
    if value < 0 or value > _MAX_UINT32:
        raise _fail_closed("capability field %s out of uint32 range", key)
    return value


def _json_int64(obj: Mapping[str, object], key: str) -> int:
    value = obj.get(key)
    if value is None:
        return 0
    if isinstance(value, bool) or not isinstance(value, int):
        raise _fail_closed("capability field %s is not an integer", key)
    if value < -_MAX_INT64 - 1 or value > _MAX_INT64:
        raise _fail_closed("capability field %s out of int64 range", key)
    return value


def _json_str_tuple(obj: Mapping[str, object], key: str) -> tuple[str, ...]:
    value = obj.get(key)
    if value is None:
        return ()
    if not isinstance(value, list):
        raise _fail_closed("capability field %s is not a list", key)
    for item in value:
        if not isinstance(item, str):
            raise _fail_closed("capability field %s contains a non-string", key)
    return tuple(value)


@dataclass(slots=True)
class Capability:
    """写入 etcd lease key 的审计记录。

    ★ JSON 字段名 = Go struct tag,**逐字节契约**:Go 的激活工具要 Unmarshal 它,
    Python 也要 Unmarshal Go 写下的记录(同 Pod 接管预检)。
    """

    service: str = ""
    instance_uid: str = ""
    writer_epoch: int = 0
    supported_policy_generation: int = 0
    supported_policy_id: str = ""
    acquired_policy_generation: int = 0
    acquired_policy_id: str = ""
    image_digest: str = ""
    keyset_revision: str = ""
    etcd_identity_revision: str = ""
    started_at_ms: int = 0
    features: tuple[str, ...] = ()

    def to_json_bytes(self) -> bytes:
        """按 Go struct 的**字段顺序**与 `omitempty` 语义序列化。

        `separators=(",", ":")` 对齐 Go `json.Marshal` 的无空白输出。
        """
        payload: dict[str, object] = {
            "service": self.service,
            "instance_uid": self.instance_uid,
            "writer_epoch": self.writer_epoch,
            "supported_policy_generation": self.supported_policy_generation,
            "supported_policy_id": self.supported_policy_id,
            "acquired_policy_generation": self.acquired_policy_generation,
        }
        if self.acquired_policy_id:  # json:"...,omitempty"
            payload["acquired_policy_id"] = self.acquired_policy_id
        payload["image_digest"] = self.image_digest
        payload["keyset_revision"] = self.keyset_revision
        if self.etcd_identity_revision:  # json:"...,omitempty"
            payload["etcd_identity_revision"] = self.etcd_identity_revision
        payload["started_at_ms"] = self.started_at_ms
        if self.features:  # json:"...,omitempty"
            payload["features"] = list(self.features)
        return json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode()

    @classmethod
    def from_json(cls, raw: bytes | str) -> Capability:
        """解析 Go / Python 写下的 capability。

        对齐 `json.Unmarshal`:未知字段忽略、缺失字段取零值、**类型不符报错**。
        类型不符必须报错而不是取零值 —— 取零值会让"记录被污染"看起来像"字段没写",
        而接管判定正是靠这些字段逐条相等。
        """
        try:
            obj = json.loads(raw)
        except asyncio.CancelledError:
            raise
        except (ValueError, TypeError) as exc:
            raise _fail_closed(
                "dsauthfence: stale capability unparsable, refuse takeover: %s", exc
            ) from exc
        if not isinstance(obj, dict):
            raise _fail_closed(
                "dsauthfence: stale capability unparsable, refuse takeover: not an object"
            )
        return cls(
            service=_json_str(obj, "service"),
            instance_uid=_json_str(obj, "instance_uid"),
            writer_epoch=_json_uint32(obj, "writer_epoch"),
            supported_policy_generation=_json_uint32(obj, "supported_policy_generation"),
            supported_policy_id=_json_str(obj, "supported_policy_id"),
            acquired_policy_generation=_json_uint32(obj, "acquired_policy_generation"),
            acquired_policy_id=_json_str(obj, "acquired_policy_id"),
            image_digest=_json_str(obj, "image_digest"),
            keyset_revision=_json_str(obj, "keyset_revision"),
            etcd_identity_revision=_json_str(obj, "etcd_identity_revision"),
            started_at_ms=_json_int64(obj, "started_at_ms"),
            features=_json_str_tuple(obj, "features"),
        )


# ── 配置 ────────────────────────────────────────────────────────────────────


@dataclass(slots=True)
class ClientSecurity:
    """etcd 客户端的生产传输与最小权限证明。

    Username/Password 是可选的:部署可以只用基于客户端证书 CN 的身份,也可以叠加
    etcd v3 用户认证。`require_auth` 只要求服务端 auth 已启用,不强迫两种身份并存。
    """

    require_mtls: bool = False
    ca_file: str = ""
    cert_file: str = ""
    key_file: str = ""
    server_name: str = ""
    client_identity: str = ""
    identity_revision: str = ""
    username_file: str = ""
    password_file: str = ""
    require_auth: bool = False
    forbidden_read_prefix: str = ""

    def enabled(self) -> bool:
        return bool(
            self.require_mtls
            or self.require_auth
            or self.ca_file
            or self.cert_file
            or self.key_file
            or self.server_name
            or self.client_identity
            or self.identity_revision
            or self.username_file
            or self.password_file
            or self.forbidden_read_prefix
        )


@dataclass(slots=True)
class Config:
    """单个进程注册 capability 所需的不可伪造运行时信息。"""

    endpoints: list[str] = field(default_factory=list)
    prefix: str = ""
    service: str = ""
    instance_uid: str = ""
    image_digest: str = ""
    keyset_revision: str = ""
    writer_epoch: int = 0
    lease_ttl_sec: int = 0
    dial_timeout_sec: float = 0.0
    security: ClientSecurity = field(default_factory=ClientSecurity)
    features: tuple[str, ...] = ()


@dataclass(slots=True)
class RuntimeConfig:
    """业务 main 已有配置可直接映射的部分;不可变 Pod 身份只从环境读。"""

    endpoints: list[str] = field(default_factory=list)
    prefix: str = ""
    service: str = ""
    keyset_revision: str = ""
    lease_ttl_sec: int = 0
    dial_timeout_sec: float = 0.0
    writer_epoch: int = 0
    features: tuple[str, ...] = ()


def normalize(cfg: Config) -> None:
    if not cfg.prefix:
        cfg.prefix = DEFAULT_PREFIX
    if cfg.lease_ttl_sec <= 0:
        cfg.lease_ttl_sec = DEFAULT_LEASE_TTL_SEC
    if cfg.dial_timeout_sec <= 0:
        cfg.dial_timeout_sec = DEFAULT_DIAL_TIMEOUT_SEC


def validate(cfg: Config) -> None:
    if not cfg.endpoints:
        raise _invalid("dsauthfence: empty endpoints")
    if not cfg.service or "/" in cfg.service:
        raise _invalid("dsauthfence: invalid service")
    if cfg.service not in REQUIRED_POLICY_V2_FEATURES:
        raise _invalid(
            "dsauthfence: service %r is not in the production writer policy", cfg.service
        )
    if not cfg.instance_uid or "/" in cfg.instance_uid:
        raise _invalid("dsauthfence: invalid instance uid")
    if cfg.writer_epoch == 0:
        raise _invalid("dsauthfence: writer epoch is zero")
    if cfg.writer_epoch < 0 or cfg.writer_epoch > _MAX_UINT32:
        raise _invalid("dsauthfence: writer epoch out of uint32 range")
    if not DIGEST_PATTERN.match(cfg.image_digest):
        raise _invalid("dsauthfence: image digest must be sha256:<64 lowercase hex>")
    if not cfg.keyset_revision:
        raise _invalid("dsauthfence: empty keyset revision")
    validate_features(cfg.features)


def validate_features(features: Iterable[str]) -> None:
    seen: set[str] = set()
    for feature in features:
        if (
            not feature
            or feature.strip() != feature
            or not CAPABILITY_FEATURE_PATTERN.match(feature)
        ):
            raise _invalid("dsauthfence: invalid capability feature")
        if feature in seen:
            raise _invalid("dsauthfence: duplicate capability feature")
        seen.add(feature)


def equal_feature_set(actual: Sequence[str], expected: Sequence[str]) -> bool:
    """两个 feature 集合是否**精确**相等(不是子集/超集)。"""
    try:
        validate_features(actual)
        validate_features(expected)
    except asyncio.CancelledError:
        raise
    except FenceError:
        return False
    if len(actual) != len(expected):
        return False
    expected_set = set(expected)
    return all(feature in expected_set for feature in actual)


def validate_required_policy_for_capability(
    state: RequiredState, service: str, writer_epoch: int, features: Sequence[str]
) -> None:
    """required 策略与本 capability 是否互相合法。任何不符一律 fail-closed。"""
    if state.raw_value == "" or state.epoch == 0:
        raise _fail_closed("dsauthfence: empty required policy state")
    v2_features = REQUIRED_POLICY_V2_FEATURES.get(service)
    if v2_features is None:
        raise _fail_closed("dsauthfence: unknown writer service %r", service)
    v3_features = REQUIRED_POLICY_V3_FEATURES.get(service, ())

    if state.policy_generation == REQUIRED_POLICY_GENERATION_V1:
        if (
            state.epoch != 1
            or state.raw_value != "1"
            or state.policy_id != ""
            or writer_epoch < 1
        ):
            raise _fail_closed("dsauthfence: invalid baseline required policy state")
        return

    if state.policy_generation == REQUIRED_POLICY_GENERATION_V2:
        if (
            state.epoch != PROTOCOL_EPOCH_V2
            or not _same_secret(state.raw_value, REQUIRED_VALUE_V2)
            or not _same_secret(state.policy_id, REQUIRED_POLICY_V2)
            or writer_epoch != PROTOCOL_EPOCH_V2
        ):
            raise _fail_closed("dsauthfence: required v2 policy or writer epoch mismatch")
        if equal_feature_set(features, v2_features):
            return
        # 唯一一条不可变的下一策略的**预置**是精确匹配,不是"超集例外":它让候选
        # hub writer 能以 V2 注册,好让 V2→V3 激活审计到它;任意增删 feature 一律失败。
        if service == "hub_allocator" and equal_feature_set(features, v3_features):
            return
        raise _fail_closed(
            "dsauthfence: service %s does not advertise exact V2 or staged V3 features",
            service,
        )

    if state.policy_generation == REQUIRED_POLICY_GENERATION_V3:
        if (
            state.epoch != PROTOCOL_EPOCH_V2
            or not _same_secret(state.raw_value, REQUIRED_VALUE_V3)
            or not _same_secret(state.policy_id, REQUIRED_POLICY_V3)
            or writer_epoch != PROTOCOL_EPOCH_V2
        ):
            raise _fail_closed("dsauthfence: required v3 policy or writer epoch mismatch")
        if not equal_feature_set(features, v3_features):
            raise _fail_closed(
                "dsauthfence: service %s does not advertise the exact %s feature policy",
                service,
                REQUIRED_POLICY_V3,
            )
        return

    raise _fail_closed(
        "dsauthfence: unsupported required policy generation %d", state.policy_generation
    )


def _same_secret(left: str, right: str) -> bool:
    """身份 / 策略串的定长比较 —— 对齐 §「加密比较用 hmac.compare_digest」。

    这些串本身不是密钥,但它们是**准入判定的依据**;用常数时间比较不吃亏,
    而且能顺手排除"以 == 比较 bytes 与 str 恒 False"这类类型混淆。
    """
    return hmac.compare_digest(left.encode(), right.encode())


# ── 激活策略(供 dsauthfence_activate 复用)─────────────────────────────────


def validate_activation_policy(
    epoch: int, services: Mapping[str, int], features: Mapping[str, set[str]]
) -> None:
    """让激活工具使用与运行期 acquire **完全相同**的固定生产服务/feature 策略。"""
    if epoch != PROTOCOL_EPOCH_V2:
        raise _invalid("unsupported activation policy epoch %d", epoch)
    validate_activation_policy_generation(
        REQUIRED_POLICY_GENERATION_V2, services, features
    )


def validate_activation_policy_generation(
    generation: int, services: Mapping[str, int], features: Mapping[str, set[str]]
) -> None:
    """校验某个激活候选所用的**精确**不可变 feature 策略。"""
    expected_policy = required_features_for_policy_generation(generation)
    policy_id = required_policy_id_for_generation(generation)
    if len(services) != len(expected_policy):
        raise _fail_closed("activation service set does not match %s", policy_id)
    for service, expected in expected_policy.items():
        if services.get(service, 0) <= 0:
            raise _fail_closed(
                "activation service %s missing from %s", service, policy_id
            )
        actual_set = features.get(service) or set()
        if len(actual_set) != len(expected):
            raise _fail_closed(
                "activation feature policy for %s does not match %s", service, policy_id
            )
        for feature in expected:
            if feature not in actual_set:
                raise _fail_closed(
                    "activation feature policy for %s misses %s", service, feature
                )
    if (
        generation == REQUIRED_POLICY_GENERATION_V3
        and services.get("hub_allocator", 0) != 1
    ):
        raise _fail_closed(
            "activation policy %s requires exactly one hub_allocator writer", policy_id
        )
    for service in features:
        if service not in expected_policy:
            raise _fail_closed(
                "activation feature policy contains unknown service %s", service
            )


def expected_services_hash(services: Mapping[str, int]) -> str:
    """对激活清单做确定性摘要,供 activation record 审计。

    ★ Go 用 `sort.Strings`(按字节序);Python `sorted()` 按码点序 —— 服务名是
    ASCII 标识符,两者结果相同。非 ASCII 服务名会让两栈摘要分叉,所以这里直接拒。
    """
    keys = sorted(services)
    for key in keys:
        if not key.isascii():
            raise _invalid("activation service name must be ASCII: %r", key)
    body = "".join(f"{key}={services[key]}\n" for key in keys)
    return hashlib.sha256(body.encode()).hexdigest()


# ── Backend 抽象(隔离 etcd 细节,允许确定性 fake 验证全部 fail-closed 分支)──


@dataclass(slots=True)
class RequiredRead:
    """`GetRequired` 的线性读结果。"""

    state: RequiredState
    watch_revision: int
    mod_revision: int
    found: bool


@dataclass(slots=True)
class CapabilityRead:
    """`GetCapability` 的线性读结果(同 Pod 崩溃重启的安全接管预检)。"""

    value: bytes
    mod_revision: int
    lease_id: int
    found: bool


@dataclass(slots=True)
class RequiredEvent:
    """required key 的有序 watch 事件。"""

    state: RequiredState = RequiredState()
    revision: int = 0
    deleted: bool = False
    err: BaseException | None = None


class Lease(Protocol):
    """capability 的存活权。`lost` 置位后进程不得继续处理受保护写请求。"""

    @property
    def lost(self) -> asyncio.Event:
        """失租事件。★ 置位是**单调终态**,不会被后续成功续租复活。"""
        ...

    def holding(self) -> bool:
        """此刻是否**可证明**仍持有(未失租 且 在本地安全窗内)。"""
        ...

    async def close(self) -> None:
        """主动释放。幂等;主动关闭不触发 `lost`。"""
        ...


class Backend(Protocol):
    """隔离 etcd 细节的后端。"""

    async def get_required(self, key: str) -> RequiredRead: ...

    async def get_capability(self, key: str) -> CapabilityRead: ...

    async def acquire_capability(
        self,
        key: str,
        lock_key: str,
        req_key: str,
        expected_required_value: str,
        expected_required_mod_revision: int,
        value: bytes,
        ttl: int,
        prev_mod_revision: int,
        prev_lease_id: int,
    ) -> Lease: ...

    def watch_required(self, key: str, revision: int) -> AsyncIterator[RequiredEvent]: ...

    async def close(self) -> None: ...


# ── Holder ──────────────────────────────────────────────────────────────────


class Holder:
    """持有 capability 租约及进程内**只增不减**的 required 高水位。"""

    __slots__ = (
        "_backend",
        "_lease",
        "_required",
        "_policy",
        "_reclaimed",
        "_lost",
        "_lost_reason",
        "_intentional",
        "_closed",
        "_tasks",
    )

    def __init__(self, backend: Backend, lease: Lease, *, reclaimed: bool) -> None:
        self._backend = backend
        self._lease = lease
        self._required = 0
        self._policy = 0
        self._reclaimed = reclaimed
        self._lost = asyncio.Event()
        self._lost_reason = ""
        self._intentional = False
        self._closed = False
        self._tasks: list[asyncio.Task] = []

    # ── 对外查询 ────────────────────────────────────────────────────────

    def required_epoch(self) -> int:
        """本进程已观察到的全局单调高水位。"""
        return self._required

    def required_policy_generation(self) -> int:
        """不可变控制面策略代。V2 与 V3 都要求数据面 writer_epoch=2。"""
        return self._policy

    @property
    def reclaimed(self) -> bool:
        """本次注册是否通过同 Pod 安全接管取得(供启动日志 / 审计观测)。"""
        return self._reclaimed

    @property
    def lost(self) -> asyncio.Event:
        """任何 fencing 条件失效时置位。调用方必须停止服务并退出。"""
        return self._lost

    def lost_reason(self) -> str:
        """本次失效的分支标识(LOST_REASON_* 之一);尚未失效时返回 ""。

        ★ reason 先写、`lost` 后置位 —— 任何看到 `lost.is_set()` 的协程都能读到非空
        reason(与 Go 的 `lostOnce` 内顺序同义)。
        """
        return self._lost_reason

    def holding(self) -> bool:
        """此刻是否**可证明**仍持有 capability。

        ★ 两条判据缺一不可:①未收到任何 lost 信号;②租约**本地安全窗**未越线。
        只看 ① 会漏掉"续租已连续失败、但循环还没来得及置位"的窗口,
        而那正好是双写者最可能发生的一段。
        """
        return not self._lost.is_set() and self._lease.holding()

    def require_holding(self) -> None:
        """受保护写的前置断言:证明不了就抛 —— 自 fencing 的调用点入口。"""
        if not self.holding():
            raise CapabilityLostError(self._lost_reason or LOST_REASON_LEASE_KEEPALIVE_ENDED)

    # ── 内部 ────────────────────────────────────────────────────────────

    def _signal_lost(self, reason: str) -> None:
        """幂等地记录原因并置位 `lost`。reason 必须**先于**置位写入。"""
        if self._lost.is_set():
            return
        self._lost_reason = reason
        self._lost.set()
        plog.get().error(
            "dsauthfence_capability_lost",
            reason=reason,
            required_epoch=self._required,
            policy_generation=self._policy,
            hint="capability fencing 条件已失效,必须停止受保护写并退出进程",
        )

    async def _monitor_lease(self) -> None:
        await self._lease.lost.wait()
        if not self._intentional:
            self._signal_lost(LOST_REASON_LEASE_KEEPALIVE_ENDED)

    async def _monitor_required(
        self,
        watch: AsyncIterator[RequiredEvent],
        service: str,
        supported: int,
        features: tuple[str, ...],
        initial_revision: int,
        initial_policy_generation: int,
    ) -> None:
        seen_revision = initial_revision
        seen_policy_generation = initial_policy_generation
        try:
            async for event in watch:
                if self._intentional:
                    return
                if event.err is not None:
                    self._signal_lost(LOST_REASON_REQUIRED_WATCH_ERROR)
                    return
                if event.deleted or event.state.epoch == 0 or event.state.raw_value == "":
                    self._signal_lost(LOST_REASON_REQUIRED_DELETED)
                    return
                if (
                    event.revision <= seen_revision
                    or event.state.epoch < self._required
                    or event.state.epoch > supported
                    or event.state.policy_generation <= seen_policy_generation
                    or not _policy_ok(event.state, service, supported, features)
                ):
                    self._signal_lost(LOST_REASON_REQUIRED_REGRESSED)
                    return
                # capability 的取得与**旧的** raw required 值 CAS 绑定。即便是唯一
                # 规范的 V1→V2 / V1→V3 / V2→V3 推进,也强制重启,好让替换的 capability
                # 以新 raw value 重新注册;沿用旧租约会让审计含糊。
                seen_revision = event.revision
                seen_policy_generation = event.state.policy_generation
                self._required = event.state.epoch
                self._policy = event.state.policy_generation
                self._signal_lost(LOST_REASON_REQUIRED_ADVANCED)
                return
            if not self._intentional:
                # watch 静默结束也不能继续写;不以重连 / 旧缓存冒充授权。
                self._signal_lost(LOST_REASON_REQUIRED_WATCH_CLOSED)
        finally:
            await _aclose(watch)

    async def close(self) -> None:
        """主动释放 capability 并停止 watch。幂等;主动关闭不触发 `lost`。"""
        if self._closed:
            return
        self._closed = True
        self._intentional = True
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        self._tasks.clear()
        with contextlib.suppress(Exception):
            await self._lease.close()
        with contextlib.suppress(Exception):
            await self._backend.close()


def _policy_ok(
    state: RequiredState, service: str, supported: int, features: Sequence[str]
) -> bool:
    try:
        validate_required_policy_for_capability(state, service, supported, features)
    except asyncio.CancelledError:
        raise
    except FenceError:
        return False
    return True


async def _aclose(iterator: AsyncIterator[RequiredEvent]) -> None:
    """关掉 watch 异步生成器。取消路径上不能因收尾再抛。"""
    aclose = getattr(iterator, "aclose", None)
    if aclose is None:
        return
    with contextlib.suppress(asyncio.CancelledError, Exception):
        await aclose()


# ── 启动 ────────────────────────────────────────────────────────────────────


async def start(backend: Backend, cfg: Config) -> Holder:
    """用已构造的 Backend 启动栅栏。生产走 `acquire`;测试可注入 fake。"""
    if backend is None:
        raise _invalid("dsauthfence: nil backend")
    normalize(cfg)
    try:
        validate(cfg)
    except asyncio.CancelledError:
        raise
    except BaseException:
        await _close_quietly(backend)
        raise

    req_key = required_key(cfg.prefix)
    try:
        # ★ 有界超时(§9 不变量 19/20):etcd 卡住不能变成"启动期永久挂起"。
        read = await asyncio.wait_for(
            backend.get_required(req_key), timeout=cfg.dial_timeout_sec
        )
    except asyncio.CancelledError:
        raise
    except BaseException as exc:
        await _close_quietly(backend)
        raise _unavailable(
            "dsauthfence: linearizable read required: %s", exc
        ) from exc

    if not read.found or read.state.epoch == 0 or read.state.raw_value == "":
        await _close_quietly(backend)
        raise _fail_closed(
            "dsauthfence: required writer epoch missing; explicit bootstrap is required"
        )
    if read.state.epoch > cfg.writer_epoch:
        await _close_quietly(backend)
        raise _fail_closed(
            "dsauthfence: required writer epoch %d exceeds supported %d",
            read.state.epoch,
            cfg.writer_epoch,
        )
    try:
        validate_required_policy_for_capability(
            read.state, cfg.service, cfg.writer_epoch, cfg.features
        )
    except asyncio.CancelledError:
        raise
    except BaseException:
        await _close_quietly(backend)
        raise

    capability = Capability(
        service=cfg.service,
        instance_uid=cfg.instance_uid,
        writer_epoch=cfg.writer_epoch,
        supported_policy_generation=REQUIRED_POLICY_GENERATION_V3,
        supported_policy_id=REQUIRED_POLICY_V3,
        acquired_policy_generation=read.state.policy_generation,
        acquired_policy_id=read.state.policy_id,
        image_digest=cfg.image_digest,
        keyset_revision=cfg.keyset_revision,
        etcd_identity_revision=cfg.security.identity_revision,
        # 审计字段,用墙钟(与 Go 的 time.Now().UnixMilli() 同);安全截止线**另用单调钟**。
        started_at_ms=int(time.time() * 1000),
        features=tuple(cfg.features),
    )
    payload = capability.to_json_bytes()

    try:
        lease, reclaimed = await _acquire_with_same_pod_takeover(
            backend, cfg, req_key, read.state.raw_value, read.mod_revision, payload
        )
    except asyncio.CancelledError:
        raise
    except BaseException as exc:
        await _close_quietly(backend)
        raise _fail_closed("dsauthfence: acquire capability: %s", exc) from exc

    holder = Holder(backend, lease, reclaimed=reclaimed)
    holder._required = read.state.epoch
    holder._policy = read.state.policy_generation
    watch = backend.watch_required(req_key, read.watch_revision + 1)
    features = tuple(cfg.features)
    # ★ 后台循环一律走 safego 且**带名字**:裸 create_task 的异常会躺在 Task 里没人取,
    # 那条监视循环已经死了而进程照常 SERVING、全程零日志(见 pandorapy/safego.py)。
    holder._tasks.append(
        safego.spawn(_TASK_MONITOR_LEASE, holder._monitor_lease)
    )
    holder._tasks.append(
        safego.spawn(
            _TASK_MONITOR_REQUIRED,
            lambda: holder._monitor_required(
                watch,
                cfg.service,
                cfg.writer_epoch,
                features,
                read.mod_revision,
                read.state.policy_generation,
            ),
        )
    )
    plog.get().info(
        "dsauthfence_capability_acquired",
        service=cfg.service,
        instance_uid=cfg.instance_uid,
        required_epoch=read.state.epoch,
        policy_generation=read.state.policy_generation,
        reclaimed=reclaimed,
        lease_ttl_sec=cfg.lease_ttl_sec,
    )
    return holder


async def _close_quietly(backend: Backend) -> None:
    with contextlib.suppress(Exception):
        await backend.close()


async def _acquire_with_same_pod_takeover(
    backend: Backend,
    cfg: Config,
    req_key: str,
    required_raw_value: str,
    required_mod_revision: int,
    payload: bytes,
) -> tuple[Lease, bool]:
    """注册 capability,并在「同 Pod 上一进程 fatal 崩溃(无法执行清理)残留同身份
    capability」时做安全接管,消除等旧租约 TTL 自然过期的恢复空窗
    (§16.8:恢复最坏耗时不得吃光业务安全租约)。

    接管**不放宽**单 writer / 防脑裂:
      - key 按 (service, PodUID) 唯一,异 Pod 副本各持异 key,永不互相接管;
      - 只有 `validate_same_pod_takeover` 全部身份字段一致才接管,任何不一致 fail-closed;
      - 接管是 ModRevision 精确 CAS(并发接管者最多一个成功)+ 与 required / activation
        lock 同一事务判定,与全新注册走**完全相同**的 fencing 条件;
      - 接管成功后 revoke 旧租约:旧进程若理论上仍存活,其续租立即终结 → 触发退出,
        结构性保证不出现第二个自认持有 capability 的进程。

    两次尝试仅覆盖「预检 Get 与注册 Txn 之间残留租约恰好自然过期 / 键变化」的窄竞态,
    把一次容器级 CrashLoop 退避(≥10s)收敛为一次进程内重读。
    """
    cap_key = capability_key(cfg.prefix, cfg.service, cfg.instance_uid)
    last_exc: BaseException | None = None
    for _attempt in range(2):
        try:
            pre = await asyncio.wait_for(
                backend.get_capability(cap_key), timeout=cfg.dial_timeout_sec
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            raise _unavailable(
                "linearizable read own capability: %s", exc
            ) from exc

        if pre.found:
            # 身份不符一律**立即**失败,不进重试:重试解决不了身份冲突,只会
            # 把"别人的 capability 还在"这条硬失败拖成两倍超时。
            validate_same_pod_takeover(pre.value, cfg)
            prev_mod_revision, prev_lease_id = pre.mod_revision, pre.lease_id
        else:
            prev_mod_revision, prev_lease_id = 0, 0

        try:
            lease = await asyncio.wait_for(
                backend.acquire_capability(
                    cap_key,
                    activation_lock_key(cfg.prefix),
                    req_key,
                    required_raw_value,
                    required_mod_revision,
                    payload,
                    cfg.lease_ttl_sec,
                    prev_mod_revision,
                    prev_lease_id,
                ),
                timeout=cfg.dial_timeout_sec,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            last_exc = exc
            continue
        return lease, pre.found
    if last_exc is None:
        # 走不到:循环体只有 return / continue 两条出口,continue 前必写 last_exc。
        # 不用 assert —— `python -O` 会把 assert 整条删掉,那时这里会掉出函数返回
        # None,调用方拿到一个「注册成功但没有租约」的 Holder(最坏的那种成功)。
        raise _fail_closed("dsauthfence: acquire capability exhausted without error")
    raise last_exc


def validate_same_pod_takeover(prev_raw: bytes, cfg: Config) -> None:
    """判定残留 capability 是否属于「同一 Pod 的上一个进程」。

    同 PodUID ⇒ kubelet 串行重启同一容器 ⇒ 同一不可变 Pod spec(镜像 digest 一致)
    且旧进程必已退出,接管不会产生第二 writer。任何字段不一致(异身份写入、镜像已换、
    配置漂移)一律拒绝接管并 fail-closed,等旧租约自然过期或人工介入,**不得放宽**。
    """
    prev = Capability.from_json(prev_raw)
    if not _same_secret(prev.service, cfg.service) or not _same_secret(
        prev.instance_uid, cfg.instance_uid
    ):
        raise _fail_closed(
            "dsauthfence: stale capability identity %s/%s mismatch, refuse takeover",
            prev.service,
            prev.instance_uid,
        )
    if not _same_secret(prev.image_digest, cfg.image_digest):
        raise _fail_closed(
            "dsauthfence: stale capability image digest mismatch, refuse takeover"
        )
    if prev.writer_epoch != cfg.writer_epoch:
        raise _fail_closed(
            "dsauthfence: stale capability writer epoch mismatch, refuse takeover"
        )
    if not _same_secret(prev.keyset_revision, cfg.keyset_revision):
        raise _fail_closed(
            "dsauthfence: stale capability keyset revision mismatch, refuse takeover"
        )


async def acquire(cfg: Config) -> Holder:
    """连接 etcd 并启动生产栅栏。所有启动期读取均为默认线性一致读。"""
    normalize(cfg)
    validate(cfg)
    client = await new_etcd_client(
        cfg.endpoints, cfg.dial_timeout_sec, cfg.prefix, cfg.security
    )
    return await start(_EtcdBackend(client), cfg)


async def acquire_runtime(runtime: RuntimeConfig) -> Holder:
    """从 Downward API 环境组装完整身份并启动栅栏。

    ★ 绝不回退 hostname 或 image tag:两者都可伪造 / 可漂移,而 capability key 的
    唯一性正建立在 PodUID 上。
    """
    security = client_security_from_env()
    return await acquire(
        Config(
            endpoints=list(runtime.endpoints),
            prefix=runtime.prefix,
            service=runtime.service,
            instance_uid=os.getenv(ENV_POD_UID, ""),
            image_digest=os.getenv(ENV_IMAGE_DIGEST, ""),
            keyset_revision=runtime.keyset_revision,
            writer_epoch=runtime.writer_epoch,
            lease_ttl_sec=runtime.lease_ttl_sec,
            dial_timeout_sec=runtime.dial_timeout_sec,
            security=security,
            features=tuple(runtime.features),
        )
    )


# ── etcd Backend ────────────────────────────────────────────────────────────


class _EtcdLease:
    """capability 的 etcd 租约 + 自驱动续租循环(aetcd 没有自动 KeepAlive)。

    与 Go `etcdLease` 的差别只在**谁来续**:Go 由 clientv3 的 KeepAlive 流负责,
    流断即失租;这里由 `_keepalive_loop` 负责,判定规则见模块头。
    """

    __slots__ = (
        "_client",
        "_lease",
        "_ttl",
        "_lost",
        "_self_fenced",
        "_safe_deadline",
        "_task",
        "_intentional",
        "_closed",
    )

    def __init__(self, client: aetcd.Client, lease: object, ttl: int) -> None:
        self._client = client
        self._lease = lease
        self._ttl = ttl
        self._lost = asyncio.Event()
        self._self_fenced = False
        margin = ttl / _SAFETY_MARGIN_DIVISOR
        # ★ 单调钟。墙钟回拨(NTP 步进 / 虚机快照恢复)会让安全窗直接失效,
        # 而失效的表现是"本地以为还持有" —— 正好是最危险的方向。
        self._safe_deadline = time.monotonic() + ttl - margin
        self._task: asyncio.Task | None = None
        self._intentional = False
        self._closed = False

    @property
    def lost(self) -> asyncio.Event:
        return self._lost

    def holding(self) -> bool:
        """未失租 且 未越过本地安全截止线。★ 越线即**单调终态**。"""
        if self._lost.is_set() or self._self_fenced:
            return False
        if time.monotonic() >= self._safe_deadline:
            # 观察到越线就地自 fencing —— 不能是"这次 False、下次可能又 True"。
            self._self_fenced = True
            return False
        return True

    def start_keepalive(self) -> None:
        if self._task is None:
            self._task = safego.spawn(_TASK_LEASE_KEEPALIVE, self._keepalive_loop)

    async def _keepalive_loop(self) -> None:
        interval = self._ttl / _KEEPALIVE_DIVISOR
        margin = self._ttl / _SAFETY_MARGIN_DIVISOR
        while True:
            sleep_for = min(interval, max(0.05, self._safe_deadline - time.monotonic()))
            await asyncio.sleep(sleep_for)
            started_at = time.monotonic()
            try:
                # ★ 有界超时:续租请求本身不能比它要证明的窗口还慢。
                await refresh_or_raise(self._lease, timeout=interval)
            except asyncio.CancelledError:
                raise
            except LeaseGoneError as exc:
                # ★ 服务端明确回复 lease 不存在 = capability key 已经不是我的了。
                # 这是**证据**不是抖动,所以不走安全窗重试:此刻可能已有接管者。
                self._declare_lost(reason="lease_gone", err=str(exc))
                return
            except BaseException as exc:  # noqa: BLE001 —— 续租失败一律按"未证明"处置
                # 续租不确定(超时 / UNKNOWN / 连接断)**必须按失败处理**,
                # 禁止乐观当成功。还在安全窗内可以再试(etcd 短抖动很常见);
                # 越线就必须放弃 —— 此时无法证明 lease 仍有效。
                if time.monotonic() < self._safe_deadline and not self._self_fenced:
                    continue
                self._declare_lost(reason="deadline_exceeded", err=repr(exc))
                return
            if self._self_fenced:
                # 迟到的成功响应**不得复活**已自 fencing 的租约:那一刻起
                # 接管者可能已经在写,复活等于制造第二个写者。
                self._declare_lost(reason="self_fenced", err="late refresh after fence")
                return
            # ★ 只有成功响应才推进安全线,且从**发起时刻**算起(保守方向) ——
            # 服务端可能在请求刚到达时就重置了 TTL,按较早时刻算不会高估剩余时间。
            self._safe_deadline = started_at + self._ttl - margin

    def _declare_lost(self, *, reason: str, err: str) -> None:
        # 主动关闭(优雅下线)不是失租:置位 lost 会让调用方把一次正常滚更
        # 当成 fencing 事故告警。与 Go 的 etcdLease.intentional 同义。
        self._self_fenced = True
        if self._intentional:
            return
        plog.get().error(
            "dsauthfence_lease_lost",
            reason=reason,
            err=err,
            ttl_sec=self._ttl,
            hint="capability 租约已不可证明,必须停止受保护写并退出进程(§9.22)",
        )
        self._lost.set()

    async def close(self) -> None:
        """主动释放:停续租 + revoke。幂等;主动关闭不置位 `lost`。"""
        if self._closed:
            return
        self._closed = True
        self._intentional = True
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._task
        with contextlib.suppress(Exception):
            await self._client.revoke_lease(self._lease.id)


class _EtcdBackend:
    """aetcd 后端。所有读走默认线性一致读(aetcd 的 `serializable=False` 默认值)。"""

    __slots__ = ("_client",)

    def __init__(self, client: aetcd.Client) -> None:
        self._client = client

    async def get_required(self, key: str) -> RequiredRead:
        got = await self._client.get(key.encode())
        if got is None:
            # Go 在 key 不存在时返回 (zero, header.Revision, 0, false, nil);
            # aetcd 的 get 在 miss 时**没有 header**,拿不到 revision。这不影响正确性:
            # 调用方对 not found 一律 fail-closed(required 缺失 = 拒绝启动),
            # 那条路径不会用到 watch revision。
            return RequiredRead(RequiredState(), 0, 0, False)
        state = parse_required_state(got.value)
        return RequiredRead(state, got.header.revision, got.mod_revision, True)

    async def get_capability(self, key: str) -> CapabilityRead:
        got = await self._client.get(key.encode())
        if got is None:
            return CapabilityRead(b"", 0, 0, False)
        return CapabilityRead(got.value, got.mod_revision, got.lease, True)

    async def acquire_capability(
        self,
        key: str,
        lock_key: str,
        req_key: str,
        expected_required_value: str,
        expected_required_mod_revision: int,
        value: bytes,
        ttl: int,
        prev_mod_revision: int,
        prev_lease_id: int,
    ) -> Lease:
        client = self._client
        txn = client.transactions
        key_b = key.encode()
        grant = await client.lease(ttl)
        try:
            # 默认要求 key 不存在;同 Pod 安全接管(prev_mod_revision>0)改为
            # ModRevision 精确 CAS,并发接管者最多一个成功,fencing 语义与全新注册一致。
            own_key_cmp = (
                txn.mod(key_b) == prev_mod_revision
                if prev_mod_revision > 0
                else txn.create(key_b) == 0
            )
            succeeded, _ = await client.transaction(
                compare=[
                    own_key_cmp,
                    # 激活工具持锁期间禁止新 writer 注册,封住 capability 审计到
                    # 推进 CAS 的 TOCTOU。
                    txn.create(lock_key.encode()) == 0,
                    # required 的线性读与 capability 注册必须组成**同一个** fencing
                    # 判定。若两者之间已发生激活 / 回退 / 删除,本次旧快照注册必须失败,
                    # 由进程退出后重新启动读取。
                    txn.value(req_key.encode()) == expected_required_value.encode(),
                    txn.mod(req_key.encode()) == expected_required_mod_revision,
                ],
                success=[txn.put(key_b, value, lease=grant.id)],
                failure=[],
            )
        except asyncio.CancelledError:
            raise
        except BaseException:
            with contextlib.suppress(Exception):
                await client.revoke_lease(grant.id)
            raise
        if not succeeded:
            with contextlib.suppress(Exception):
                await client.revoke_lease(grant.id)
            raise CapabilityFencedError(key)
        if prev_lease_id != 0:
            # 接管成功后终结旧租约:Put 已把 key 挂到新租约(etcd 覆盖写自动解除旧
            # 租约附着),此刻 revoke 只会杀掉空租约的续租 —— 若旧进程理论上仍存活,
            # 它会立刻失租并退出,结构性保证单 writer;旧租约已自然过期时返回
            # NotFound,忽略即可。
            with contextlib.suppress(Exception):
                await client.revoke_lease(prev_lease_id)
        lease = _EtcdLease(client, grant, ttl)
        lease.start_keepalive()
        return lease

    async def watch_required(
        self, key: str, revision: int
    ) -> AsyncIterator[RequiredEvent]:
        """有序 watch。任何错误都作为一条 `RequiredEvent(err=...)` 交出后结束。

        ★ 不能吞掉错误后静默 return:那会走到「watch 静默结束」分支,把
        "watch 报错(可能是 compact revision)"与"通道自然结束"混成一条,
        丢掉 2026-07-29 事故要的那格取证信息。
        """
        watch = None
        try:
            watch = await self._client.watch(key.encode(), start_revision=revision)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001 —— 建流失败也要交出错误事件
            yield RequiredEvent(err=exc)
            return
        try:
            async for event in watch:
                deleted = event.kind == _rtypes.EventKind.DELETE
                item = RequiredEvent(revision=event.kv.mod_revision, deleted=deleted)
                if not deleted:
                    try:
                        item.state = parse_required_state(event.kv.value)
                    except asyncio.CancelledError:
                        raise
                    except BaseException as exc:  # noqa: BLE001
                        item.err = exc
                yield item
                if item.err is not None:
                    return
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001 —— 流上的错误同样必须交出去
            yield RequiredEvent(err=exc)
        finally:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await watch.cancel()

    async def close(self) -> None:
        with contextlib.suppress(Exception):
            await self._client.close()


# ── 安全姿态(security.go)──────────────────────────────────────────────────


def _strict_bool_env(name: str) -> bool:
    """只接受 "" / "0" / "1"。★ 不用 `bool(os.getenv(...))`:那样 "false" 也是真。"""
    value = (os.getenv(name) or "").strip()
    if value in ("", "0"):
        return False
    if value == "1":
        return True
    raise _invalid("dsauthfence: %s must be empty, 0, or 1", name)


def client_security_from_env() -> ClientSecurity:
    """只读取路径 / 开关,**不读取也不回显凭据内容**。

    生产 Pod 由 revisioned immutable Secret 注入这些路径;本地无这些环境变量时
    保持旧的明文开发路径。
    """
    require_mtls = _strict_bool_env(ENV_ETCD_REQUIRE_MTLS)
    require_auth = _strict_bool_env(ENV_ETCD_REQUIRE_AUTH)
    return ClientSecurity(
        require_mtls=require_mtls,
        ca_file=(os.getenv(ENV_ETCD_CA_FILE) or "").strip(),
        cert_file=(os.getenv(ENV_ETCD_CERT_FILE) or "").strip(),
        key_file=(os.getenv(ENV_ETCD_KEY_FILE) or "").strip(),
        server_name=(os.getenv(ENV_ETCD_SERVER_NAME) or "").strip(),
        client_identity=(os.getenv(ENV_ETCD_CLIENT_IDENTITY) or "").strip(),
        identity_revision=(os.getenv(ENV_ETCD_IDENTITY_REVISION) or "").strip(),
        username_file=(os.getenv(ENV_ETCD_USERNAME_FILE) or "").strip(),
        password_file=(os.getenv(ENV_ETCD_PASSWORD_FILE) or "").strip(),
        require_auth=require_auth,
        forbidden_read_prefix=(os.getenv(ENV_ETCD_FORBIDDEN_READ_PREFIX) or "").strip(),
    )


def validate_client_security(
    endpoints: Sequence[str], allowed_prefix: str, security: ClientSecurity
) -> None:
    """安全 etcd 配置的全量校验。逐条对应 Go 的 validateClientSecurity。"""
    if not security.require_mtls:
        raise _invalid("dsauthfence: secure etcd configuration requires mTLS")
    for name, value in (
        ("custom CA file", security.ca_file),
        ("client certificate file", security.cert_file),
        ("client key file", security.key_file),
        ("server name", security.server_name),
        ("client certificate identity", security.client_identity),
        ("identity revision", security.identity_revision),
    ):
        if value.strip() == "":
            raise _invalid("dsauthfence: missing %s", name)
    if not ETCD_IDENTITY_REVISION_PATTERN.match(security.identity_revision):
        raise _invalid("dsauthfence: etcd identity revision must be canonical rN")
    if (security.username_file == "") != (security.password_file == ""):
        raise _invalid(
            "dsauthfence: username/password files must be configured together"
        )
    if security.require_auth and security.forbidden_read_prefix == "":
        raise _invalid("dsauthfence: auth proof requires a forbidden read prefix")
    if security.forbidden_read_prefix != "":
        allowed = clean_prefix(allowed_prefix)
        forbidden = clean_prefix(security.forbidden_read_prefix)
        if (
            allowed == forbidden
            or allowed.startswith(forbidden)
            or forbidden.startswith(allowed)
        ):
            raise _invalid(
                "dsauthfence: forbidden read prefix overlaps the allowed DS auth prefix"
            )
    for endpoint in endpoints:
        _validate_canonical_https_endpoint(endpoint)


def _validate_canonical_https_endpoint(endpoint: str) -> None:
    try:
        parsed = urllib.parse.urlparse(endpoint)
        hostname, port_text = parsed.hostname, parsed.port and str(parsed.port)
    except ValueError as exc:
        raise _invalid(
            "dsauthfence: production etcd endpoint must be canonical https://host:port"
        ) from exc
    raw_port = ""
    if parsed.netloc.rfind(":") > parsed.netloc.rfind("]"):
        raw_port = parsed.netloc[parsed.netloc.rfind(":") + 1 :]
    if (
        parsed.scheme != "https"
        or not hostname
        or not raw_port
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path != ""
        or parsed.query != ""
        or parsed.fragment != ""
    ):
        raise _invalid(
            "dsauthfence: production etcd endpoint must be canonical https://host:port"
        )
    # ★ 端口必须是**规范**十进制:"0080" 与 "80" 指同一端口但字节不同,而运维比对的是
    # 字节。Go 用 `strconv.FormatUint(port,10) != parsed.Port()` 判,这里等价照抄。
    if not raw_port.isdigit() or not raw_port.isascii():
        raise _invalid(
            "dsauthfence: production etcd endpoint must use a canonical TCP port in 1..65535"
        )
    port = int(raw_port)
    if port == 0 or port > 65535 or str(port) != raw_port or port_text is None:
        raise _invalid(
            "dsauthfence: production etcd endpoint must use a canonical TCP port in 1..65535"
        )


@dataclass(slots=True)
class TLSMaterial:
    """已校验的 mTLS 材料。对应 Go 的 `*tls.Config`(此处只承载已验过的事实)。"""

    ca_pem: bytes
    cert_file: str
    key_file: str
    server_name: str
    common_name: str
    minimum_version: int = ssl.TLSVersion.TLSv1_2


def load_tls_material(security: ClientSecurity) -> TLSMaterial:
    """读取并校验 CA / 客户端证书。逐条对应 Go 的 loadTLSConfig。

    校验的四件事(缺一条就等于没校验):CA 里确实有证书、证书 / 私钥能配对加载、
    **证书当前在有效期内**、**CN 与配置的身份一致**。
    """
    from cryptography import x509
    from cryptography.hazmat.primitives import serialization

    try:
        ca_pem = _read_file_bytes(security.ca_file)
    except OSError as exc:
        raise _invalid("dsauthfence: read custom CA: %s", exc) from exc
    try:
        roots = x509.load_pem_x509_certificates(ca_pem)
    except asyncio.CancelledError:
        raise
    except BaseException as exc:  # noqa: BLE001 —— 解析失败与"空 CA"同等处置
        raise _invalid("dsauthfence: custom CA contains no certificates") from exc
    if not roots:
        raise _invalid("dsauthfence: custom CA contains no certificates")

    try:
        cert_pem = _read_file_bytes(security.cert_file)
        key_pem = _read_file_bytes(security.key_file)
    except OSError as exc:
        raise _invalid(
            "dsauthfence: load client certificate/key: %s", exc
        ) from exc
    try:
        chain = x509.load_pem_x509_certificates(cert_pem)
        serialization.load_pem_private_key(key_pem, password=None)
    except asyncio.CancelledError:
        raise
    except BaseException as exc:  # noqa: BLE001
        raise _invalid("dsauthfence: load client certificate/key: %s", exc) from exc
    if not chain:
        raise _invalid("dsauthfence: client certificate chain is empty")

    leaf = chain[0]
    import datetime as _dt

    now = _dt.datetime.now(tz=_dt.UTC)
    if now < leaf.not_valid_before_utc or now >= leaf.not_valid_after_utc:
        raise _invalid("dsauthfence: client certificate is not currently valid")
    common_names = leaf.subject.get_attributes_for_oid(x509.NameOID.COMMON_NAME)
    cn = str(common_names[0].value) if common_names else ""
    if not _same_secret(cn, security.client_identity):
        raise _invalid(
            "dsauthfence: client certificate CN does not match configured identity"
        )
    return TLSMaterial(
        ca_pem=ca_pem,
        cert_file=security.cert_file,
        key_file=security.key_file,
        server_name=security.server_name,
        common_name=cn,
    )


def _read_file_bytes(path: str) -> bytes:
    with open(os.path.normpath(path), "rb") as handle:
        return handle.read()


def read_credential_file(path: str, kind: str) -> str:
    """读取 etcd 用户名 / 口令文件。空、带首尾空白、含 NUL/CR/LF 一律拒。"""
    try:
        raw = _read_file_bytes(path)
    except OSError as exc:
        raise _invalid("dsauthfence: read etcd %s file: %s", kind, exc) from exc
    value = raw.decode("utf-8", "replace")
    if value == "" or value.strip() != value or any(c in value for c in "\x00\r\n"):
        raise _invalid("dsauthfence: etcd %s file is empty or non-canonical", kind)
    return value


async def new_etcd_client(
    endpoints: Sequence[str],
    timeout_sec: float,
    prefix: str,
    security: ClientSecurity,
) -> aetcd.Client:
    """构造 etcd 客户端。

    ★ 安全档 fail-closed(见模块头):`aetcd.Client.connect()` 无条件走
    `rpc.insecure_channel`,**没有任何**参数能让它做 mTLS 或校验服务端证书。
    因此这里先做与 Go 完全一致的全量配置校验(配置写错要在启动期就红),
    再**明确拒绝连接** —— 绝不悄悄退回明文:那会让一个自以为有 mTLS + 最小权限的
    部署在毫无信号的情况下裸奔,而这正是本模块要防的那类静默失效。
    """
    if security.enabled():
        validate_client_security(endpoints, prefix, security)
        load_tls_material(security)
        if security.username_file:
            read_credential_file(security.username_file, "username")
            read_credential_file(security.password_file, "password")
        raise SecureEtcdUnsupportedError(
            "dsauthfence: secure etcd (mTLS) is not supported by the aetcd client; "
            "refusing to fall back to an insecure channel"
        )
    host, _, port = endpoints[0].rpartition(":")
    if not port.isdigit() or not port.isascii():
        raise _invalid(
            "dsauthfence: etcd endpoint must be host:port, got %r", endpoints[0]
        )
    client = aetcd.Client(
        host=host or "127.0.0.1", port=int(port), timeout=max(1, int(timeout_sec))
    )
    await client.connect()
    return client


async def dial_secure_etcd_client(
    endpoints: Sequence[str], timeout_sec: float, prefix: str
) -> aetcd.Client:
    """以与 `acquire_runtime` 完全相同的安全姿态连接 etcd。

    供同仓其它栅栏件复用,保证所有 DS 授权体系的 etcd 客户端走同一套安全构造,
    不允许旁路明文路径分叉。
    """
    return await new_etcd_client(
        endpoints, timeout_sec, prefix, client_security_from_env()
    )
