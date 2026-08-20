"""真 Agones `GameServerAllocator` —— 对应 Go 侧
`services/battle/ds_allocator/internal/data/agones_allocator.go`。

用 k8s apiserver REST 直连 `allocation.agones.dev/v1` GameServerAllocation,不引入
agones / client-go 重依赖。Agones 的分配 API 与 k8s provider 无关(ACK / 自建 /
minikube 上的 Agones controller 一致),故本实现 provider-agnostic。

职责切分(与 Go 逐一对应):

  - `allocate`                       legacy 单阶段:POST GSA,取 gameServerName + addr:port
  - `allocate_authoritative`         Model B:GSA POST **永不带令牌**,选中后严格 GET 回读
  - `resolve_allocation_by_id`       结果未知的 POST 的**只读**对账(零对象 = 权威缺席)
  - `deliver_credential`             uid + resourceVersion 双 CAS 投递凭据 annotation
  - `release_expected` / `release`   exact 实例回收 / legacy 幂等 DELETE
  - `list_allocated_game_servers`    孤儿对账清扫的只读列举(分页,fail-closed)
  - `delete_allocated_game_server_exact`  exact 复核 + 双 precondition 删除
  - `list_fleet_capacities`          Fleet 容量巡检快照

── 为什么直接打 k8s REST,而不是用 `kubernetes` / `kubernetes_asyncio` ──────────
与 `hub_allocator/agones_fleet.py` 同一理由:本文件只用到 6 个 REST 端点
(POST gameserverallocations、get/list/patch/delete gameserver、get pod、get fleet),
换成官方 client 会为此拖进一整套 model 代码生成 + 版本矩阵,而条件 PATCH 的
RFC-6902 语义、DeleteOptions 的 preconditions 还是得自己拼。走 `httpx.AsyncClient`,
额外好处是测试可以直接注入 `httpx.MockTransport` 当假 apiserver。

── 三处**改一个字符就打穿不变量**的地方 ────────────────────────────────────
① **label / annotation key 逐字符照抄**。`pandora.dev/allocation-id` 是不确定分配
   对账与孤儿回收的唯一 fencing token;写错一个字符,selector 选不中任何对象,
   于是"零对象 = 权威缺席"这条结论会把一台**已经分配出去**的 Pod 判成没分配过,
   上游删 claim 后再分配第二个 Pod —— §9 不变量 1(一人一 DS)当场破。
② **DELETE 的 UID + resourceVersion 双 precondition**。只带 UID 挡不住
   "GET→DELETE 窗口内该 GS 被一次新的 GSA 分配"(GSA 必然 PATCH metadata 抬高
   resourceVersion);少了 rv 这条,孤儿回收会删掉一台刚刚载人的 Allocated DS。
   反过来只带 rv 也不够:同名重建的新对象 rv 从头计数。两条缺一不可。
③ **PATCH / DELETE 的 HTTP 结果永远不是事实**。2xx、409、transport timeout 一律
   要再做一次严格 GET 回读确认。写成"2xx 即成功"会在一次网络抖动后重签一张新令牌
   把已经送达的旧令牌覆盖掉;写成"超时即失败"会在删除已被受理时重复 DELETE。

── §9.22 fail-closed:查询不确定**绝不**冒充"没有" ──────────────────────────
`list_allocated_game_servers` 任一页失败就整轮抛错(半份清单会把仍被引用的 GS 误判
成孤儿);`resolve_allocation_by_id` 的 LIST 失败抛错而不是返回"没找到";
`_do` 的传输错误塞进 `KubeResponse.error` 而不是当空响应。
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import pathlib
import ssl
import time
import urllib.parse
import uuid as _uuid
from typing import Any, Awaitable, Callable

import httpx

from pandorapy import dsmetadata, errcode
from pandorapy import log as plog
from pandorapy import releasetrack
from pandorapy.services.ds_allocator import conf as dconf

# ── GameServer 标签 / annotation key ─────────────────────────────────────────
#
# ★ 逐字符照抄 Go 的字面量。抽成常量是为了让机械对拍能盯住(测试直接对着这些常量
#   造假对象),**不是**为了"以后好改" —— 这些 key 是与 UE DS、运维 selector、
#   Grafana 查询共享的跨仓协议,改了任何一个字符都会静默选不中对象。

#: Agones Fleet 给其 GameServer 打的标签 key(selector 用)。
FLEET_LABEL_KEY = "agones.dev/fleet"
#: 同时用作精确 selector 与实际轨 annotation 审计字段(§9.21 灰度轨道粘滞)。
RELEASE_TRACK_METADATA_KEY = "pandora.dev/release-track"
#: 权威花名册(升序去重的十进制 player_id,逗号分隔)。
BATTLE_ROSTER_ANNOTATION_KEY = "pandora.dev/roster"
#: 权威阵营(`pid=faction,...`,按 roster 升序)。缺它 DS 会 RejectSpawn(进图没角色)。
BATTLE_COMBAT_FACTIONS_ANNOTATION_KEY = "pandora.dev/combat-factions"
#: 本次 GSA 写入的唯一 UUIDv4;不确定分配对账与孤儿回收的唯一 fencing token。
BATTLE_ALLOCATION_METADATA_KEY = "pandora.dev/allocation-id"
#: 下发 DS 回调令牌的 GameServer annotation key。
#: label 有 63 字符 / 字符集限制放不下 JWT,annotation 无此限制。
DS_TOKEN_ANNOTATION_KEY = "pandora.dev/ds-token"

#: 业务标识 label(打到被分配的 GameServer 上,便于运维 / 排障关联对局)。
MATCH_ID_LABEL_KEY = "pandora.dev/match-id"
MAP_ID_LABEL_KEY = "pandora.dev/map-id"
GAME_MODE_LABEL_KEY = "pandora.dev/game-mode"

# ── Agones 分配 / 回收链路的拒绝 reason 枚举(infra.md §11.3 R2)────────────
#
# 为什么必须由这一层自己打:本文件的所有失败最终只以**一个 err 字符串**冒泡到 biz 的
# gameserver_allocate_failed —— selector 选了哪几个 Fleet、apiserver 回了什么 status、
# Agones 说的是 UnAllocated(没空闲)还是 Contention(抢占冲突)、严格回读时到底是哪个
# annotation 对不上,全都留在这里。缺了它们,"玩家进不去副本"在后端只看得到一句
# "分配失败",分不清是容量不足、Fleet 名写错,还是 apiserver 抖动。
#
# snake_case 常量,稳定不变(日志系统按 msg+reason 聚合告警)。

# 分配 POST 与入参。
AGONES_REJECT_TRACK_INVALID = "release_track_invalid"
AGONES_REJECT_FLEET_NOT_CONFIGURED = "fleet_not_configured"
AGONES_REJECT_MARSHAL_FAILED = "request_marshal_failed"
AGONES_REJECT_TRANSPORT_FAILED = "apiserver_call_failed"
AGONES_REJECT_HTTP_ERROR = "apiserver_http_error"
AGONES_REJECT_DECODE_FAILED = "response_decode_failed"
AGONES_REJECT_NO_AVAILABLE = "no_available_gameserver"
AGONES_REJECT_STATUS_INCOMPLETE = "allocation_status_incomplete"
# Model B 权威分配的入参与选中后严格回读。
AGONES_REJECT_ALLOCATION_ID_INVALID = "allocation_id_invalid"
AGONES_REJECT_ROSTER_INVALID = "roster_invalid"
AGONES_REJECT_FACTIONS_MISSING = "combat_factions_missing"
AGONES_REJECT_FACTIONS_INVALID = "combat_factions_invalid"
AGONES_REJECT_STRICT_GET_GS_FAILED = "strict_get_gameserver_failed"
AGONES_REJECT_BINDING_MISMATCH = "gameserver_binding_mismatch"
AGONES_REJECT_STRICT_GET_POD_FAILED = "strict_get_pod_failed"
AGONES_REJECT_POD_OWNER_MISMATCH = "pod_owner_mismatch"
# 不确定分配的只读对账。
AGONES_REJECT_RESOLVE_LIST_FAILED = "resolve_list_failed"
AGONES_REJECT_RESOLVE_AMBIGUOUS = "allocation_id_ambiguous"
AGONES_REJECT_RESOLVE_BINDING = "resolved_binding_mismatch"
AGONES_REJECT_RESOLVE_POD = "resolved_pod_mismatch"
# 凭据投递与回收。
AGONES_REJECT_CREDENTIAL_UNCONFIRMED = "credential_patch_unconfirmed"
AGONES_REJECT_RELEASE_POD_UID_MISSING = "durable_pod_uid_missing"
AGONES_REJECT_RELEASE_NOT_CONFIRMED = "release_not_confirmed"
AGONES_REJECT_RELEASE_PROBE_FAILED = "release_pre_probe_failed"
AGONES_REJECT_RELEASE_HTTP_ERROR = "release_http_error"
AGONES_REJECT_POD_UID_PREFLIGHT = "pod_uid_preflight_mismatch"

#: Agones 控制器依据 SDK health ping 断流写下的判死状态;进入该状态的 GameServer
#: 不再参与分配,随后被 GameServerSet 删除替换。
AGONES_STATE_UNHEALTHY = "Unhealthy"
#: Agones GameServer 的已分配状态。
AGONES_STATE_ALLOCATED = "Allocated"

#: 单次 apiserver 响应的读上限。超限时 `_do` 显式报错而不是静默截断:
#: 半截 JSON 只会以 "unexpected end of JSON input" 冒泡,永远查不到真因是清单太大。
#: 大清单必须靠分页而不是提高本值。
AGONES_MAX_RESPONSE_BYTES = 1 << 20

#: GameServer 分页 LIST 的每页对象数。单个 GameServer 序列化后约 4~8 KB
#: (含整份 pod template、managedFields 与 pandora 注解,2026-08-03 本机实测
#: 4.1~5.8 KB),100 个/页 ≈ 0.4~0.8 MiB,稳在 `AGONES_MAX_RESPONSE_BYTES` 之内。
AGONES_LIST_PAGE_SIZE = 100

_HTTP_NOT_FOUND = 404
_HTTP_CONFLICT = 409

_UINT32_MAX = (1 << 32) - 1
_UINT64_MAX = (1 << 64) - 1


class AgonesError(RuntimeError):
    """Agones / k8s REST 交互失败。对应 Go 侧那一族裸 `fmt.Errorf("agones: ...")`。

    与 `errcode.PandoraError` 的分工照抄 Go:内部辅助函数(getGameServer / getPod /
    probeExactResource / list…)返回**裸 error**,由公开方法在边界上包成 errcode。
    这样"哪一层的错"在 Python 侧仍然一眼能分辨。
    """


class ReleaseDeletionPendingError(RuntimeError):
    """exact 删除已被 apiserver 受理(对象带 deletionTimestamp),物理消失需等待 Pod
    terminationGracePeriod(默认 30s)。对应 Go 的哨兵 `ErrReleaseDeletionPending`。

    调用方应按分配身份退避后重试确认,**不得重复 DELETE**,更不得据此写 teardown
    proof —— 只有 `release_expected` 正常返回才是物理消失证明。

    ★ 为什么是独立异常类型而不是一个 errcode:Go 那边它是 `errors.New` 哨兵,靠
      `errors.Is` 识别,与 errcode 体系刻意分开(它不是"业务失败",而是"还没完")。
      塌进 `ErrDSAllocationFailed` 会让调用方把"等宽限期"当成"回收失败"去重试删除。
    """


class BattleAllocationError(errcode.PandoraError):
    """带**部分分配结果**的分配失败。对应 Go 的 `return partial, err`。

    ★ 为什么必须有这个类:Go 的 `AllocateAuthoritative` 在 POST 结果不可解析时仍然
      `return partial, err` —— `allocation_id` 是未知结果对账 / 回收的唯一 fencing
      token,返回 nil 会让调用方删 claim 后再次分配第二个 Pod(§9 不变量 1 破)。
      Python 用异常传播,那个 partial 必须显式挂在异常上,否则**静默丢失**。

    ★ 用**声明式 `__slots__` 子类**而不是 `setattr`:后者能写进去(Exception 自带
      `__dict__`)但拼错一个字母不会报错,读的那侧只会永远拿到 None。
    """

    __slots__ = ("allocation",)

    def __init__(
        self,
        code: int,
        msg: str = "",
        *args: object,
        allocation: "AuthoritativeGameServerAllocation | None" = None,
        cause: BaseException | None = None,
    ) -> None:
        super().__init__(code, msg, *args, cause=cause)
        self.allocation = allocation


# ── 入参边界检查 ────────────────────────────────────────────────────────────
#
# Go 的形参是 uint64 / uint32,负数与超界在那边根本编译不出来。Python 的 int 无限
# 精度,不显式挡就会静默走进 label 值里变成 "-1" / "18446744073709551616",
# 于是 selector 永远选不中、回读永远 mismatch,而**没有任何一处报错**。


def _require_uint64(name: str, value: int) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or not 0 <= value <= _UINT64_MAX:
        raise errcode.PandoraError(
            errcode.ErrInvalidArg, "agones: %s %r out of uint64 range", name, value
        )
    return value


def _require_uint32(name: str, value: int) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or not 0 <= value <= _UINT32_MAX:
        raise errcode.PandoraError(
            errcode.ErrInvalidArg, "agones: %s %r out of uint32 range", name, value
        )
    return value


def _parse_canonical_uuid_v4(s: str) -> _uuid.UUID | None:
    """严格解析规范 UUIDv4 文本。对应 Go 的四条连判:

        uuid.Parse(s) 成功 && != uuid.Nil && Version()==4 && Variant()==RFC4122
        && parsed.String() == s

    ★ 最后那条 `String() == s` 是关键:Go 的 `uuid.Parse` 也吃 `{...}` / `urn:uuid:`
      / 无连字符的 32 hex,Python 的 `uuid.UUID()` 同样宽容。只有再要求"回吐的规范
      小写连字符形式逐字等于入参",两栈才对同一批字符串给出同一个接受集 —— 而这个
      集合会被写进 k8s label(`pandora.dev/allocation-id`),两栈不一致就意味着
      Go 分配的对象 Python 对账不出来。
    """
    if not isinstance(s, str) or s == "":
        return None
    try:
        parsed = _uuid.UUID(s)
    except (ValueError, AttributeError, TypeError):
        return None
    if parsed.int == 0 or parsed.version != 4 or parsed.variant != _uuid.RFC_4122:
        return None
    if str(parsed) != s:
        return None
    return parsed


# ── GameServer / Pod / Fleet 响应 JSON(只声明用到的字段)────────────────────
#
# 解析刻意**严格**:字段类型不对就抛,而不是宽容地当空值。Go 的 json.Unmarshal 本来
# 就是这个行为,而这里的宽容会把「apiserver 回了个我们看不懂的东西」悄悄变成
# 「这台 GameServer 没有 annotation」,再变成「回读判定通过」。


def _as_str(raw: dict[str, Any], key: str) -> str:
    value = raw.get(key)
    if value is None:
        return ""
    if not isinstance(value, str):
        raise AgonesError(f"agones: field {key!r} must be a string, got {type(value).__name__}")
    return value


def _as_str_map(raw: dict[str, Any], key: str) -> dict[str, str] | None:
    """返回 `None` 表示该字段整体缺失 —— 对应 Go 的 nil map。

    ★ `None` 与 `{}` 必须区分:`AuthoritativeGameServerAllocation.annotations_present`
      用它决定 `deliver_credential` 是 `add /metadata/annotations`(整体创建)还是
      逐 key `add`。对一个**不存在**的 annotations 逐 key add,RFC-6902 会因父路径
      不存在而整片失败。
    """
    value = raw.get(key)
    if value is None:
        return None
    if not isinstance(value, dict):
        raise AgonesError(f"agones: field {key!r} must be an object")
    out: dict[str, str] = {}
    for k, v in value.items():
        if not isinstance(k, str) or not isinstance(v, str):
            raise AgonesError(f"agones: field {key!r} must map string to string")
        out[k] = v
    return out


@dataclasses.dataclass(slots=True)
class GSMetadata:
    name: str = ""
    uid: str = ""
    resource_version: str = ""
    labels: dict[str, str] = dataclasses.field(default_factory=dict)
    #: `None` = 对象上根本没有 annotations 字段(Go 的 nil map)。
    annotations: dict[str, str] | None = None
    #: 非空 = 删除已受理、对象处于终止宽限(graceful termination);它不是物理消失
    #: 证明,但足以证明无需再发 DELETE。
    deletion_timestamp: str = ""

    def label(self, key: str) -> str:
        return self.labels.get(key, "")

    def ann(self, key: str) -> str:
        """读 annotation;缺 annotations 或缺 key 都返回 ""(Go 读 nil map 的语义)。"""
        if self.annotations is None:
            return ""
        return self.annotations.get(key, "")


@dataclasses.dataclass(slots=True)
class GameServerResponse:
    metadata: GSMetadata = dataclasses.field(default_factory=GSMetadata)
    state: str = ""


@dataclasses.dataclass(slots=True)
class OwnerReference:
    api_version: str = ""
    kind: str = ""
    name: str = ""
    uid: str = ""
    controller: bool = False


@dataclasses.dataclass(slots=True)
class PodResponse:
    name: str = ""
    uid: str = ""
    resource_version: str = ""
    owner_references: list[OwnerReference] = dataclasses.field(default_factory=list)


def _parse_game_server(raw: Any) -> GameServerResponse:
    if not isinstance(raw, dict):
        raise AgonesError("agones: gameserver payload is not an object")
    md_raw = raw.get("metadata") or {}
    if not isinstance(md_raw, dict):
        raise AgonesError("agones: gameserver metadata is not an object")
    st_raw = raw.get("status") or {}
    if not isinstance(st_raw, dict):
        raise AgonesError("agones: gameserver status is not an object")
    return GameServerResponse(
        metadata=GSMetadata(
            name=_as_str(md_raw, "name"),
            uid=_as_str(md_raw, "uid"),
            resource_version=_as_str(md_raw, "resourceVersion"),
            labels=_as_str_map(md_raw, "labels") or {},
            annotations=_as_str_map(md_raw, "annotations"),
            deletion_timestamp=_as_str(md_raw, "deletionTimestamp"),
        ),
        state=_as_str(st_raw, "state"),
    )


def _parse_pod(raw: Any) -> PodResponse:
    if not isinstance(raw, dict):
        raise AgonesError("agones: pod payload is not an object")
    md_raw = raw.get("metadata") or {}
    if not isinstance(md_raw, dict):
        raise AgonesError("agones: pod metadata is not an object")
    owners_raw = md_raw.get("ownerReferences") or []
    if not isinstance(owners_raw, list):
        raise AgonesError("agones: pod metadata.ownerReferences is not an array")
    owners: list[OwnerReference] = []
    for o in owners_raw:
        if not isinstance(o, dict):
            raise AgonesError("agones: pod ownerReferences item is not an object")
        controller = o.get("controller", False)
        if not isinstance(controller, bool):
            raise AgonesError("agones: pod ownerReferences.controller must be a boolean")
        owners.append(
            OwnerReference(
                api_version=_as_str(o, "apiVersion"),
                kind=_as_str(o, "kind"),
                name=_as_str(o, "name"),
                uid=_as_str(o, "uid"),
                controller=controller,
            )
        )
    return PodResponse(
        name=_as_str(md_raw, "name"),
        uid=_as_str(md_raw, "uid"),
        resource_version=_as_str(md_raw, "resourceVersion"),
        owner_references=owners,
    )


# ── 值对象 ──────────────────────────────────────────────────────────────────


@dataclasses.dataclass(slots=True, frozen=True)
class BattleFleetRoute:
    """map_id → stable/canary 专属预热 Fleet。对应 Go 的 `battleFleetRoute`。"""

    stable: str = ""
    canary: str = ""


@dataclasses.dataclass(slots=True)
class AuthoritativeGameServerAllocation:
    """Model B 分配结果。对应 Go 的同名结构。

    `instance_uid` / `resource_version` **只**来自选中后的严格 GameServer GET;
    `annotations_present` 决定 JSON Patch 应新增整个 map 还是单独成员。
    """

    pod_name: str = ""
    addr: str = ""
    instance_uid: str = ""
    pod_uid: str = ""
    instance_epoch: int = 0  # Go: uint32
    resource_version: str = ""
    allocation_id: str = ""
    release_track: str = ""
    annotations_present: bool = False


@dataclasses.dataclass(slots=True)
class AllocatedGameServerInfo:
    """对账清扫用的 Allocated GameServer 快照。对应 Go 的同名结构。"""

    name: str = ""
    uid: str = ""
    fleet: str = ""
    #: 取自 `pandora.dev/allocation-id` label(本系统 GSA 写入的唯一 UUID);
    #: 手工 GSA / 非本系统分配可能为空。空值不参与引用匹配,只按 name/UID 判定。
    allocation_id: str = ""
    #: deletionTimestamp 非空,删除已受理,处于终止宽限,无需再处理。
    deleting: bool = False


@dataclasses.dataclass(slots=True)
class FleetCapacity:
    """一个 Agones Fleet 的容量快照(取自 Fleet spec/status)。对应 Go 的同名结构。"""

    fleet: str = ""
    replicas: int = 0  # status.replicas:当前总副本数(容量上限)
    ready: int = 0  # status.readyReplicas:空闲可分配
    allocated: int = 0  # status.allocatedReplicas:已被对局占用
    #: spec.replicas:运维 / autoscaler **期望**的副本数,与 status 三项来自同一次 GET。
    #: 用于区分「被负载打满(desired>0 且 ready==0)」与「本就没配容量(desired==0)」——
    #: INC-20260724-001:未做金丝雀发布时 canary Fleet 常态 desired=0,旧逻辑把它恒判
    #: exhausted,每 5 分钟一条 Error + Grafana critical 长期 firing,把真实的 stable
    #: ready=0 信号淹没了。
    desired: int = 0
    #: 显式区分「解码到 spec.replicas 且为 0」与「没解码到」。不得让解码失败静默取
    #: 零值 —— 那正是 §9.22 禁止的「把 UNKNOWN 冒充成确定值」。
    desired_known: bool = False
    #: 该 Fleet 属金丝雀轨。只有 canary 轨的 desired==0 才是常态(可静默);
    #: stable 轨被缩到 0 仍必须照常告警(否则运维误缩零会被静音)。
    canary: bool = False


@dataclasses.dataclass(slots=True)
class ExactResourceProbe:
    """exact 对象(按 name GET + 期望 UID 对账)的三态快照。对应 Go 的 `exactResourceProbe`。"""

    #: NotFound / 同名对象 UID 已换代:期望实例物理消失。
    gone: bool = False
    #: 同 UID 且带 deletionTimestamp:删除已受理,处于终止宽限。
    deleting: bool = False


@dataclasses.dataclass(slots=True)
class KubeResponse:
    """一次 REST 调用的结果。对应 Go 的 `(body, statusCode, transportErr)` 三返回值。

    ★ 传输错误**不抛异常**,而是塞进 `error` 字段。这不是偷懒:
      `deliver_credential` 必须在 PATCH **超时 / 连接失败**之后仍然去跑一次确认 GET
      —— 因为「请求超时」完全不能证明「服务端没应用」。写成抛异常,调用点就会在
      `await` 处直接跳走,那段 fail-closed 的确认逻辑整块失效,于是一次网络抖动就被
      当成「投递失败」,下轮重签一张新令牌把已经送达的旧令牌覆盖掉。
    """

    body: bytes = b""
    status: int = 0
    error: Exception | None = None


#: legacy DS 回调令牌签发器:match_id -> token;失败抛异常。
#: 对应 Go 的 `dsTokenIssuer func(matchID uint64) (string, error)`。
DSTokenIssuer = Callable[[int], Awaitable[str]]


# ── 模块级小工具 ────────────────────────────────────────────────────────────


def _is_2xx(status: int) -> bool:
    return 200 <= status < 300


def sanitize_label_value(s: str) -> str:
    """把 game_mode / allocation_id 收敛成合法 k8s label value。对应 Go 的
    `sanitizeLabelValue`:≤63 字符,首尾字母数字,中间允许 `-_.`;非法字符替换为
    `-`,空值 / 全非法值返回 `"unknown"`。

    ★ Go 按 **rune** 遍历、按**字节**截断(`out[:63]`),看似会切碎多字节字符 ——
      但所有非 ASCII rune 在前一步已被替换成 `-`,输出恒为纯 ASCII,字节数 == 字符数。
      Python 按字符截断因此与 Go 逐字等价,不需要再绕 encode/decode。
    """
    if s == "":
        return "unknown"
    chars: list[str] = []
    for r in s:
        if ("a" <= r <= "z") or ("A" <= r <= "Z") or ("0" <= r <= "9") or r in "-_.":
            chars.append(r)
        else:
            chars.append("-")
    out = "".join(chars)
    if len(out) > 63:
        out = out[:63]
    out = out.strip("-_.")
    if out == "":
        return "unknown"
    return out


def truncate(b: bytes, n: int) -> str:
    """截断 body 给错误信息用,避免日志过长。对应 Go 的 `truncate`。"""
    if len(b) <= n:
        return b.decode("utf-8", errors="replace")
    return b[:n].decode("utf-8", errors="replace") + "..."


def escape_json_pointer(s: str) -> str:
    """RFC 6901 转义。对应 Go 的 `strings.ReplaceAll(strings.ReplaceAll(k,"~","~0"),"/","~1")`。

    ★ 顺序不能反:必须先换 `~` 再换 `/`。反过来的话 `/` 先变成 `~1`,随后那个新生成的
      `~` 又被当成原文的 `~` 换成 `~01`,路径直接错。annotation key 里带 `/`
      (`pandora.dev/ds-token`)是常态,这条每次都会走到。
    """
    return s.replace("~", "~0").replace("/", "~1")


def pod_owned_by_game_server(pod: PodResponse | None, pod_name: str, gameserver_uid: str) -> bool:
    """对应 Go 的 `podOwnedByGameServer`。

    四条前置(名字、pod uid 非空、gs uid 非空)缺一即 false,再要求 ownerReferences
    里存在一条 `controller=true && kind=="GameServer" && uid==gameserver_uid`。
    """
    if pod is None or pod.name != pod_name or pod.uid == "" or gameserver_uid == "":
        return False
    for owner in pod.owner_references:
        if owner.controller and owner.kind == "GameServer" and owner.uid == gameserver_uid:
            return True
    return False


def confirm_credential_delivery(
    gs: GameServerResponse | None,
    allocation: AuthoritativeGameServerAllocation,
    annotations: dict[str, str],
) -> str | None:
    """严格确认凭据 annotation 已落到该 exact 实例上。对应 Go 的
    `confirmCredentialDelivery`(Go 返 error,这里返回**失败原因字符串**,`None` = 通过)。

    ★ 为什么返回字符串而不是抛:调用方 `deliver_credential` 要把它与 `patch_status` /
      `patch_err` **并列**写进一条日志和一条错误消息里(Go 那边是 `confirmErr` 变量)。
      抛异常会逼调用方在 try 里再套一层,反而更容易漏掉某条证据。
    """
    if (
        gs is None
        or gs.metadata.name != allocation.pod_name
        or gs.metadata.uid != allocation.instance_uid
        or gs.metadata.resource_version == ""
    ):
        return "gameserver identity mismatch or incomplete"
    for key in annotations:
        want = annotations[key]
        got = gs.metadata.ann(key)
        if got != want:
            return f"annotation {key!r} mismatch: got={got!r}"
    return None


def _build_ssl_context(cfg: dconf.AgonesConf) -> ssl.SSLContext | bool:
    """按 Go 的 `tls.Config` 语义建 SSL 上下文。

    ★ 提供了 CA 且能读到时,**只信这一个 CA**,不叠加系统根证书池 —— Go 那边是
      `tlsCfg.RootCAs = pool`(整体替换)。用 `ssl.create_default_context(cafile=...)`
      会在系统根之上再加,等于放宽了信任面。
    ★ CA 文件不存在 → 跳过用系统根(in-cluster 默认路径在集群外不存在,Go 同:
      `if pem, err := os.ReadFile(cfg.CAPath); err == nil`)。
    ★ CA 文件存在但**解析失败** → 抛错(Go: `return nil, fmt.Errorf("agones: parse CA %s failed")`)。
      这不是可以降级的情况:它意味着运维以为配了专用 CA,实际却在用系统根验证一个
      私有 apiserver。
    """
    if cfg.insecure_skip_tls_verify:
        return False
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.check_hostname = True
    ctx.verify_mode = ssl.CERT_REQUIRED
    pem: str | None = None
    if cfg.ca_path != "":
        try:
            pem = pathlib.Path(cfg.ca_path).read_text(encoding="ascii")
        except (OSError, UnicodeDecodeError):
            pem = None
    if pem is None:
        ctx.load_default_certs()
        return ctx
    try:
        ctx.load_verify_locations(cadata=pem)
    except (ssl.SSLError, ValueError) as exc:
        raise ValueError(f"agones: parse CA {cfg.ca_path} failed") from exc
    return ctx


# ── 分配器 ──────────────────────────────────────────────────────────────────


class AgonesGameServerAllocator:
    """经 k8s REST 调 Agones GameServerAllocation。对应 Go 的 `AgonesGameServerAllocator`。

    构造失败场景(Go 返 error,这里抛 `ValueError`,main 据此 fatal 或回退):
      - `fleet_name` 空(无法选择 GameServer)
      - `canary_percent` 超出 [0,100]
      - `canary_percent > 0` 但 `canary_fleet_name` / `canary_seed` 空
      - CA 文件配置了却解析失败
    """

    def __init__(
        self,
        cfg: dconf.AgonesConf,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        if cfg.fleet_name == "":
            raise ValueError("agones: fleet_name required when enabled")
        if cfg.canary_percent > 100:
            raise ValueError(f"agones: canary_percent={cfg.canary_percent} out of range [0,100]")
        if cfg.canary_percent > 0 and (cfg.canary_fleet_name == "" or cfg.canary_seed == ""):
            raise ValueError("agones: canary_percent>0 requires canary_fleet_name and canary_seed")

        timeout = cfg.allocate_timeout_td().total_seconds()
        if timeout <= 0:
            timeout = dconf.DEFAULT_AGONES_ALLOCATE_TIMEOUT.total_seconds()

        map_fleets: dict[int, BattleFleetRoute] = {}
        for mf in cfg.map_fleets:
            if mf.map_id > 0 and (mf.fleet_name != "" or mf.canary_fleet_name != ""):
                map_fleets[mf.map_id] = BattleFleetRoute(
                    stable=mf.fleet_name, canary=mf.canary_fleet_name
                )

        self._api_server = cfg.api_server.rstrip("/")
        self._namespace = cfg.namespace
        self._fleet_name = cfg.fleet_name
        self._canary_fleet_name = cfg.canary_fleet_name
        self._map_fleets = map_fleets
        self._advertise_host = cfg.advertise_host.strip()
        self._token_path = cfg.token_path  # "" 或 "-" → 不带 Authorization
        self._allocate_timeout = timeout

        if transport is not None:
            # 测试用假 apiserver:此时不建 TLS 上下文(MockTransport 不走网络)。
            self._client = httpx.AsyncClient(transport=transport, timeout=timeout)
        else:
            self._client = httpx.AsyncClient(verify=_build_ssl_context(cfg), timeout=timeout)

        #: 签发 DS 回调服务令牌(main 在 ds_auth.secret 已配时注入)。非 None 时
        #: `allocate` 把令牌写进 GSA 的 `metadata.annotations["pandora.dev/ds-token"]`,
        #: DS 经 Agones SDK GameServer() 读到后回调时带 Bearer 头。
        self._ds_token_issuer: DSTokenIssuer | None = None
        #: guard=enforce 时 True:签发失败必须 fail-closed(不分配无令牌的 DS,
        #: 否则该 DS 回调会被 enforce 守卫全拒,等于开了个连不回来的对局)。
        #: off/permissive 下 False,签发失败降级为无令牌分配以保对局可开。
        self._ds_token_required = False

    def set_ds_token_issuer(self, issuer: DSTokenIssuer | None, required: bool) -> None:
        """注入 DS 回调令牌签发器(可选依赖)。对应 Go 的 `SetDSTokenIssuer`。

        `required=True`(guard=enforce)时签发失败会让 `allocate` 抛错(fail-closed)。
        """
        self._ds_token_issuer = issuer
        self._ds_token_required = required

    async def aclose(self) -> None:
        """关闭底层 HTTP 连接池。"""
        await self._client.aclose()

    # ── legacy 单阶段分配 ─────────────────────────────────────────────────

    async def allocate(
        self, match_id: int, map_id: int, game_mode: str, release_track: str
    ) -> tuple[str, str, str]:
        """POST 一个 GameServerAllocation,返回 `(gameServerName, address:port, actual_track)`。

        对应 Go 的 `Allocate`。selectors 有序(Agones 按顺序尝试,选中首个有空闲
        GameServer 的):

          1. 若 map_id 配了专属预热 Fleet(map_fleets)→ 首选它
             (Pod 已预加载目标图,分配即可玩);
          2. 通用 Fleet(Loader 模式,分配后按 map-id label travel)作兜底。
        """
        match_id = _require_uint64("match_id", match_id)
        map_id = _require_uint32("map_id", map_id)
        if not releasetrack.valid(release_track):
            plog.get().warning(
                "gameserver_allocate_rejected",
                reason=AGONES_REJECT_TRACK_INVALID,
                match_id=match_id,
                map_id=map_id,
                release_track=release_track,
            )
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "agones: invalid release_track %r", release_track
            )
        labels = {
            MATCH_ID_LABEL_KEY: f"{match_id}",
            MAP_ID_LABEL_KEY: f"{map_id}",
            GAME_MODE_LABEL_KEY: sanitize_label_value(game_mode),
        }
        annotations: dict[str, str] = {}
        # DS 回调服务令牌经 annotation 下发(DS 拿不到签名密钥,只持有短期令牌)。
        # enforce(_ds_token_required=True):签发失败 fail-closed,返回分配失败,
        #   不产生连不回来的对局;
        # off/permissive:签发失败降级为无令牌分配,先保对局可开。
        if self._ds_token_issuer is not None:
            try:
                tok = await self._ds_token_issuer(match_id)
            except asyncio.CancelledError:
                raise
            except Exception as terr:  # noqa: BLE001 — 与 Go 的 `terr != nil` 同宽
                if self._ds_token_required:
                    plog.get().error(
                        "ds_callback_token_sign_failed",
                        match_id=match_id,
                        err=repr(terr),
                        mode="enforce",
                        hint="ds_auth.mode=enforce 下签发失败即 fail-closed;检查 ds_auth.secret / 签名配置",
                    )
                    raise errcode.PandoraError(
                        errcode.ErrDSAllocationFailed,
                        "ds_callback_token sign failed under enforce for match %d: %s",
                        match_id,
                        terr,
                    ) from terr
                plog.get().warning(
                    "ds_callback_token_sign_failed", match_id=match_id, err=repr(terr)
                )
            else:
                annotations = {DS_TOKEN_ANNOTATION_KEY: tok}

        return await self._allocate_with_metadata(
            match_id, map_id, release_track, labels, annotations
        )

    # ── Model B 权威分配 ──────────────────────────────────────────────────

    async def allocate_authoritative(
        self,
        match_id: int,
        allocation_id: str,
        player_ids: list[int],
        combat_faction_by_player: dict[int, int],
        map_id: int,
        game_mode: str,
        release_track: str,
    ) -> AuthoritativeGameServerAllocation:
        """Model B 的 K8s 分配半段。对应 Go 的 `AllocateAuthoritative`。

        GSA POST **永不携带令牌**;选中后必须严格 GET GameServer 取得 UID/resourceVersion,
        任一字段缺失都 fail-closed。

        失败时抛 `BattleAllocationError`,其 `.allocation` 携带**已知的部分结果**
        (至少含 `allocation_id`)—— 它是未知结果对账 / 回收的唯一 fencing token,
        丢了会让调用方删 claim 后再次分配第二个 Pod。
        """
        match_id = _require_uint64("match_id", match_id)
        map_id = _require_uint32("map_id", map_id)
        parsed = _parse_canonical_uuid_v4(allocation_id)
        if match_id == 0 or parsed is None or not releasetrack.valid(release_track):
            # R2:一个 if 收敛了"没 match_id / allocation_id 不是规范 UUIDv4 / track 非法"
            # 三类因,把判据字段都带上才能分开 —— 否则只能看到一句"参数不合法"。
            plog.get().warning(
                "gameserver_allocate_rejected",
                reason=AGONES_REJECT_ALLOCATION_ID_INVALID,
                match_id=match_id,
                allocation_id=allocation_id,
                map_id=map_id,
                release_track=release_track,
                parse_err="not a canonical uuid v4" if parsed is None else None,
            )
            raise BattleAllocationError(
                errcode.ErrInvalidArg, "agones: match_id and allocation_id required"
            )
        try:
            canonical_players, roster = dsmetadata.canonical_roster(player_ids)
        except ValueError as roster_err:
            plog.get().warning(
                "gameserver_allocate_rejected",
                reason=AGONES_REJECT_ROSTER_INVALID,
                match_id=match_id,
                allocation_id=allocation_id,
                players=len(player_ids),
                canonical_players=0,
                err=repr(roster_err),
            )
            raise BattleAllocationError(
                errcode.ErrInvalidArg, "agones: invalid battle roster: %s", roster_err
            ) from roster_err
        # 阵营与名单是同一份对局定义的两半,不可分割:一场对局就是「谁在场 + 每人属于
        # 哪一方」。此前阵营是可选的,于是"名单齐了但阵营缺失"的分配能成功下发,DS 拿到
        # 后只能退化成每人一个独立阵营的混战——队友互相能打,而且看起来一切正常
        # (能进图、能打、能结算),错误被玩成了功能。缺阵营现在与缺名单同级。
        if not combat_faction_by_player:
            plog.get().warning(
                "gameserver_allocate_rejected",
                reason=AGONES_REJECT_FACTIONS_MISSING,
                match_id=match_id,
                allocation_id=allocation_id,
                players=len(canonical_players),
            )
            raise BattleAllocationError(
                errcode.ErrInvalidArg,
                "agones: battle combat factions required for match %d",
                match_id,
            )
        try:
            canonical_players, combat_factions = dsmetadata.canonical_combat_factions(
                canonical_players, combat_faction_by_player
            )
        except ValueError as faction_err:
            plog.get().warning(
                "gameserver_allocate_rejected",
                reason=AGONES_REJECT_FACTIONS_INVALID,
                match_id=match_id,
                allocation_id=allocation_id,
                players=len(canonical_players),
                factions=len(combat_faction_by_player),
                err=repr(faction_err),
            )
            raise BattleAllocationError(
                errcode.ErrInvalidArg, "agones: invalid battle combat factions: %s", faction_err
            ) from faction_err

        partial = AuthoritativeGameServerAllocation(allocation_id=allocation_id)
        labels = {
            MATCH_ID_LABEL_KEY: f"{match_id}",
            MAP_ID_LABEL_KEY: f"{map_id}",
            GAME_MODE_LABEL_KEY: sanitize_label_value(game_mode),
            BATTLE_ALLOCATION_METADATA_KEY: sanitize_label_value(allocation_id),
        }
        annotations = {
            BATTLE_ROSTER_ANNOTATION_KEY: roster,
            BATTLE_COMBAT_FACTIONS_ANNOTATION_KEY: combat_factions,
            BATTLE_ALLOCATION_METADATA_KEY: allocation_id,
        }
        try:
            pod_name, addr, selected_track = await self._allocate_with_metadata(
                match_id, map_id, release_track, labels, annotations
            )
        except errcode.PandoraError as exc:
            # 即使 POST 没有可解析响应,也必须把 allocation_id 交还调用方。
            raise BattleAllocationError(
                exc.code, "%s", exc.msg, allocation=partial, cause=exc
            ) from exc

        partial.pod_name, partial.addr = pod_name, addr
        try:
            gs = await self._get_game_server(pod_name)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 — 与 Go 的 `err != nil` 同宽
            plog.get().warning(
                "gameserver_allocate_rejected",
                reason=AGONES_REJECT_STRICT_GET_GS_FAILED,
                match_id=match_id,
                allocation_id=allocation_id,
                pod=pod_name,
                release_track=selected_track,
                err=repr(exc),
                hint="POST 已选中但回读不到该 GameServer;该 Pod 会由 allocation_id 对账链回收",
            )
            raise BattleAllocationError(
                errcode.ErrDSAllocationFailed,
                "agones: strict GET selected gameserver %s failed: %s",
                pod_name,
                exc,
                allocation=partial,
                cause=exc,
            ) from exc

        actual_release_track = gs.metadata.label(RELEASE_TRACK_METADATA_KEY)
        if (
            gs.metadata.name != pod_name
            or gs.metadata.uid == ""
            or gs.metadata.resource_version == ""
            or gs.metadata.label(MATCH_ID_LABEL_KEY) != str(match_id)
            or gs.metadata.label(BATTLE_ALLOCATION_METADATA_KEY)
            != sanitize_label_value(allocation_id)
            or gs.metadata.ann(BATTLE_ALLOCATION_METADATA_KEY) != allocation_id
            or gs.metadata.ann(BATTLE_ROSTER_ANNOTATION_KEY) != roster
            # 阵营与名单同级精确比对:投递后回读必须逐字节一致,PATCH 丢字段或被别的
            # 写者覆盖都要在这里判定为分配失败,而不是放行一台"名单对、阵营缺"的 DS。
            or gs.metadata.ann(BATTLE_COMBAT_FACTIONS_ANNOTATION_KEY) != combat_factions
            or not releasetrack.valid(actual_release_track)
            or actual_release_track != selected_track
            or gs.metadata.ann(RELEASE_TRACK_METADATA_KEY) != actual_release_track
        ):
            # 逐项带出"期望 vs 实际",否则只能看到"binding incomplete"而不知道是名字、
            # UID、花名册、阵营还是 release_track 对不上。
            plog.get().warning(
                "gameserver_allocate_rejected",
                reason=AGONES_REJECT_BINDING_MISMATCH,
                match_id=match_id,
                allocation_id=allocation_id,
                pod=pod_name,
                got_name=gs.metadata.name,
                uid=gs.metadata.uid,
                rv=gs.metadata.resource_version,
                label_match_id=gs.metadata.label(MATCH_ID_LABEL_KEY),
                label_allocation_id=gs.metadata.label(BATTLE_ALLOCATION_METADATA_KEY),
                roster_match=gs.metadata.ann(BATTLE_ROSTER_ANNOTATION_KEY) == roster,
                factions_match=(
                    gs.metadata.ann(BATTLE_COMBAT_FACTIONS_ANNOTATION_KEY) == combat_factions
                ),
                want_release_track=selected_track,
                got_release_track=actual_release_track,
            )
            raise BattleAllocationError(
                errcode.ErrDSAllocationFailed,
                "agones: selected gameserver identity/binding incomplete: "
                "want_name=%r name=%r uid=%r rv=%r",
                pod_name,
                gs.metadata.name,
                gs.metadata.uid,
                gs.metadata.resource_version,
                allocation=partial,
            )

        partial.instance_uid = gs.metadata.uid
        partial.resource_version = gs.metadata.resource_version
        try:
            pod = await self._get_pod(pod_name)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            plog.get().warning(
                "gameserver_allocate_rejected",
                reason=AGONES_REJECT_STRICT_GET_POD_FAILED,
                match_id=match_id,
                allocation_id=allocation_id,
                pod=pod_name,
                uid=gs.metadata.uid,
                err=repr(exc),
            )
            raise BattleAllocationError(
                errcode.ErrDSAllocationFailed,
                "agones: strict GET selected pod %s failed: %s",
                pod_name,
                exc,
                allocation=partial,
                cause=exc,
            ) from exc
        if not pod_owned_by_game_server(pod, pod_name, gs.metadata.uid):
            plog.get().warning(
                "gameserver_allocate_rejected",
                reason=AGONES_REJECT_POD_OWNER_MISMATCH,
                match_id=match_id,
                allocation_id=allocation_id,
                pod=pod_name,
                pod_uid=pod.uid,
                uid=gs.metadata.uid,
            )
            raise BattleAllocationError(
                errcode.ErrDSAllocationFailed,
                "agones: selected pod identity/owner incomplete: pod=%r pod_uid=%r gameserver_uid=%r",
                pod_name,
                pod.uid,
                gs.metadata.uid,
                allocation=partial,
            )
        # R1:权威分配落定是不可逆推进(从此这台 exact 实例就是本局的唯一目标)。
        # biz 的 battle_warming 只有 pod;这里才有 uid / pod_uid / rv 三件 exact 身份,
        # 重连签票与回收 fencing 全靠它们对账。每局一条,不属高频路径。
        plog.get().info(
            "gameserver_allocate_bound",
            match_id=match_id,
            allocation_id=allocation_id,
            pod=pod_name,
            ds_addr=addr,
            uid=gs.metadata.uid,
            pod_uid=pod.uid,
            rv=gs.metadata.resource_version,
            release_track=actual_release_track,
            players=len(canonical_players),
            map_id=map_id,
        )
        return AuthoritativeGameServerAllocation(
            pod_name=pod_name,
            addr=addr,
            instance_uid=gs.metadata.uid,
            pod_uid=pod.uid,
            resource_version=gs.metadata.resource_version,
            allocation_id=allocation_id,
            release_track=actual_release_track,
            annotations_present=gs.metadata.annotations is not None,
        )

    # ── 不确定分配的只读对账 ──────────────────────────────────────────────

    async def resolve_allocation_by_id(
        self,
        match_id: int,
        allocation_id: str,
        player_ids: list[int],
        combat_faction_by_player: dict[int, int],
        map_id: int,
        game_mode: str,
    ) -> tuple[AuthoritativeGameServerAllocation | None, bool]:
        """对账一次 POST 结果未知的分配。对应 Go 的 `ResolveAllocationByID`。

        allocation_id 是任何凭据存在之前就由 GSA 请求写入的幂等 / fencing label。
        本方法**严格只读**:

          - 零对象  = 权威缺席(POST 未生效),返回 `(None, False)`;
          - 恰一对象 = 必须与原始 match/roster 元数据及其 owned Pod **完整**匹配;
          - 多对象  = 每局一台的不变量被破坏,不得猜、不得自动删,抛错等人工介入。

        ★ LIST 失败**抛错**而不是返回 `(None, False)`:把"查不到"与"查不了"混成
          一件事,就等于用一次 apiserver 抖动换来"可以安全地当作没分配过"的结论
          (§9.22 明令禁止)。
        """
        match_id = _require_uint64("match_id", match_id)
        map_id = _require_uint32("map_id", map_id)
        parsed = _parse_canonical_uuid_v4(allocation_id)
        roster = ""
        combat_factions = ""
        canonical_players: list[int] = []
        roster_ok = True
        try:
            canonical_players, roster = dsmetadata.canonical_roster(player_ids)
        except ValueError:
            roster_ok = False
        if roster_ok and combat_faction_by_player:
            try:
                canonical_players, combat_factions = dsmetadata.canonical_combat_factions(
                    canonical_players, combat_faction_by_player
                )
            except ValueError:
                roster_ok = False
        if match_id == 0 or parsed is None or not roster_ok or not canonical_players:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "agones: complete uncertain allocation identity required"
            )

        selector = BATTLE_ALLOCATION_METADATA_KEY + "=" + sanitize_label_value(allocation_id)
        list_url = (
            f"{self._api_server}/apis/agones.dev/v1/namespaces/{self._namespace}"
            f"/gameservers?labelSelector={urllib.parse.quote_plus(selector)}"
        )
        resp = await self._do("GET", list_url)
        if resp.error is not None:
            plog.get().warning(
                "gameserver_allocation_resolve_rejected",
                reason=AGONES_REJECT_RESOLVE_LIST_FAILED,
                match_id=match_id,
                allocation_id=allocation_id,
                err=repr(resp.error),
            )
            raise errcode.PandoraError(
                errcode.ErrDSAllocationFailed,
                "agones: resolve allocation_id %s: %s",
                allocation_id,
                resp.error,
                cause=resp.error,
            )
        if not _is_2xx(resp.status):
            raise errcode.PandoraError(
                errcode.ErrDSAllocationFailed,
                "agones: resolve allocation_id %s http %d: %s",
                allocation_id,
                resp.status,
                truncate(resp.body, 256),
            )
        try:
            items = _list_items(resp.body)
        except AgonesError as exc:
            raise errcode.PandoraError(
                errcode.ErrDSAllocationFailed,
                "agones: decode allocation_id %s list: %s",
                allocation_id,
                exc,
                cause=exc,
            ) from exc

        if len(items) == 0:
            # 零对象 = 权威缺席(POST 未生效)。这是"不确定分配"的唯一良性结局,
            # 也是后续"可以安全地当作没分配过"的依据,必须留证。
            plog.get().info(
                "gameserver_allocation_resolved_absent",
                match_id=match_id,
                allocation_id=allocation_id,
                map_id=map_id,
            )
            return None, False
        if len(items) > 1:
            # 同一个 allocation_id 出现多个 GameServer = 每局一台的不变量被破坏,
            # 不得猜、不得自动删,必须人工介入 —— 故这是 ERROR 而非 WARN。
            plog.get().error(
                "gameserver_allocation_resolve_rejected",
                reason=AGONES_REJECT_RESOLVE_AMBIGUOUS,
                match_id=match_id,
                allocation_id=allocation_id,
                gameservers=len(items),
                hint="同一 allocation_id 对应多台 GameServer;不自动回收,需运维确认",
            )
            raise errcode.PandoraError(
                errcode.ErrDSAllocationFailed,
                "agones: allocation_id %s is ambiguous: gameservers=%d",
                allocation_id,
                len(items),
            )

        gs = _parse_game_server(items[0])
        actual_track = gs.metadata.label(RELEASE_TRACK_METADATA_KEY)
        if (
            gs.metadata.name == ""
            or gs.metadata.uid == ""
            or gs.metadata.resource_version == ""
            or gs.metadata.label(MATCH_ID_LABEL_KEY) != str(match_id)
            or gs.metadata.label(MAP_ID_LABEL_KEY) != str(map_id)
            or gs.metadata.label(GAME_MODE_LABEL_KEY) != sanitize_label_value(game_mode)
            or gs.metadata.label(BATTLE_ALLOCATION_METADATA_KEY)
            != sanitize_label_value(allocation_id)
            or gs.metadata.ann(BATTLE_ALLOCATION_METADATA_KEY) != allocation_id
            or gs.metadata.ann(BATTLE_ROSTER_ANNOTATION_KEY) != roster
            # 与分配路径同级的精确比对。这里刻意**不**额外要求"必须非空":本方法是不确定
            # 认领的只读对账入口,若某条历史认领没带阵营,硬拒会让它永远解析不出对应
            # GameServer,从而把一台 Allocated 的 Pod 变成查不到出身的孤儿。让它按空值
            # 匹配上、进入正常回收链,比把它悬空更安全;新分配已在写入侧强制非空。
            or gs.metadata.ann(BATTLE_COMBAT_FACTIONS_ANNOTATION_KEY) != combat_factions
            or not releasetrack.valid(actual_track)
            or gs.metadata.ann(RELEASE_TRACK_METADATA_KEY) != actual_track
        ):
            plog.get().warning(
                "gameserver_allocation_resolve_rejected",
                reason=AGONES_REJECT_RESOLVE_BINDING,
                match_id=match_id,
                allocation_id=allocation_id,
                pod=gs.metadata.name,
                uid=gs.metadata.uid,
                label_match_id=gs.metadata.label(MATCH_ID_LABEL_KEY),
                label_map_id=gs.metadata.label(MAP_ID_LABEL_KEY),
                roster_match=gs.metadata.ann(BATTLE_ROSTER_ANNOTATION_KEY) == roster,
                factions_match=(
                    gs.metadata.ann(BATTLE_COMBAT_FACTIONS_ANNOTATION_KEY) == combat_factions
                ),
                release_track=actual_track,
            )
            raise errcode.PandoraError(
                errcode.ErrDSAllocationFailed,
                "agones: allocation_id %s resolved GameServer binding is incomplete or conflicting",
                allocation_id,
            )
        try:
            pod = await self._get_pod(gs.metadata.name)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            plog.get().warning(
                "gameserver_allocation_resolve_rejected",
                reason=AGONES_REJECT_STRICT_GET_POD_FAILED,
                match_id=match_id,
                allocation_id=allocation_id,
                pod=gs.metadata.name,
                err=repr(exc),
            )
            raise errcode.PandoraError(
                errcode.ErrDSAllocationFailed,
                "agones: resolve allocation_id %s pod %s: %s",
                allocation_id,
                gs.metadata.name,
                exc,
                cause=exc,
            ) from exc
        if not pod_owned_by_game_server(pod, gs.metadata.name, gs.metadata.uid):
            plog.get().warning(
                "gameserver_allocation_resolve_rejected",
                reason=AGONES_REJECT_RESOLVE_POD,
                match_id=match_id,
                allocation_id=allocation_id,
                pod=gs.metadata.name,
                pod_uid=pod.uid,
                uid=gs.metadata.uid,
            )
            raise errcode.PandoraError(
                errcode.ErrDSAllocationFailed,
                "agones: allocation_id %s associated Pod identity is incomplete",
                allocation_id,
            )
        # R1:不确定分配被对账回一台 exact 实例 = 权威结论落定(后续要么接管要么回收)。
        plog.get().info(
            "gameserver_allocation_resolved",
            match_id=match_id,
            allocation_id=allocation_id,
            pod=gs.metadata.name,
            uid=gs.metadata.uid,
            pod_uid=pod.uid,
            release_track=actual_track,
        )
        return (
            AuthoritativeGameServerAllocation(
                pod_name=gs.metadata.name,
                instance_uid=gs.metadata.uid,
                pod_uid=pod.uid,
                resource_version=gs.metadata.resource_version,
                allocation_id=allocation_id,
                release_track=actual_track,
                annotations_present=gs.metadata.annotations is not None,
            ),
            True,
        )

    # ── GSA POST ─────────────────────────────────────────────────────────

    async def _allocate_with_metadata(
        self,
        match_id: int,
        map_id: int,
        desired_release_track: str,
        labels: dict[str, str],
        annotations: dict[str, str],
    ) -> tuple[str, str, str]:
        """对应 Go 的 `allocateWithMetadata`。

        ★ canary → stable 的容量回退**只在明确的 `ErrDSNoAvailable`(UnAllocated /
          Contention)上发生**。transport / decode 等结果未知时立即停,绝不冒险产生
          第二个已分配 GameServer(§9 不变量 1)。
        """
        if not releasetrack.valid(desired_release_track):
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "agones: invalid desired release track %r",
                desired_release_track,
            )
        tracks = [desired_release_track]
        if desired_release_track == releasetrack.CANARY:
            tracks.append(releasetrack.STABLE)
        last = len(tracks) - 1
        for i, track in enumerate(tracks):
            attempt_labels = dict(labels)
            attempt_annotations = dict(annotations)
            attempt_labels[RELEASE_TRACK_METADATA_KEY] = track
            attempt_annotations[RELEASE_TRACK_METADATA_KEY] = track
            try:
                pod, addr = await self._allocate_once_with_metadata(
                    match_id, map_id, track, attempt_labels, attempt_annotations
                )
            except errcode.PandoraError as exc:
                if exc.code != errcode.ErrDSNoAvailable or i == last:
                    raise
                plog.get().warning(
                    "battle_canary_capacity_fallback_stable", match_id=match_id, map_id=map_id
                )
                continue
            return pod, addr, track
        raise errcode.PandoraError(errcode.ErrDSNoAvailable, "agones: no gameserver")

    async def _allocate_once_with_metadata(
        self,
        match_id: int,
        map_id: int,
        release_track: str,
        labels: dict[str, str],
        annotations: dict[str, str],
    ) -> tuple[str, str]:
        """对应 Go 的 `allocateOnceWithMetadata`。"""
        route = self._map_fleets.get(map_id)
        general_fleet = self._fleet_name
        dedicated_fleet = route.stable if route is not None else ""
        if release_track == releasetrack.CANARY:
            general_fleet = self._canary_fleet_name
            dedicated_fleet = route.canary if route is not None else ""
        if general_fleet == "":
            # INC-20260724-001 L8:fleet 名未配置是**配置错误**,不是容量事实。
            # 报 ErrDSNoAvailable(5001) 会把它混进"无空闲副本"的容量口径,让运维照着
            # 扩容排查。上游 matchmaker 对 5001/5002 处理完全一致(都算确定性失败),
            # 故改码不影响行为,只把语义归位。
            plog.get().warning(
                "gameserver_allocate_rejected",
                reason=AGONES_REJECT_FLEET_NOT_CONFIGURED,
                match_id=match_id,
                map_id=map_id,
                release_track=release_track,
                hint="agones.fleet_name / canary_fleet_name 未配;这是配置错误不是容量不足",
            )
            raise errcode.PandoraError(
                errcode.ErrDSAllocationFailed, "agones: no %s fleet configured", release_track
            )

        def selector_labels(fleet: str) -> dict[str, str]:
            return {FLEET_LABEL_KEY: fleet, RELEASE_TRACK_METADATA_KEY: release_track}

        selectors: list[dict[str, dict[str, str]]] = []
        if dedicated_fleet != "" and dedicated_fleet != general_fleet:
            selectors.append({"matchLabels": selector_labels(dedicated_fleet)})
        selectors.append({"matchLabels": selector_labels(general_fleet)})

        meta: dict[str, Any] = {}
        if labels:
            meta["labels"] = labels
        if annotations:
            meta["annotations"] = annotations
        req_body: dict[str, Any] = {
            "apiVersion": "allocation.agones.dev/v1",
            "kind": "GameServerAllocation",
            # 把业务标识打到被分配的 GameServer 上,便于运维 / 排障关联对局。
            "spec": {"selectors": selectors, "metadata": meta},
        }
        try:
            payload = json.dumps(req_body).encode("utf-8")
        except (TypeError, ValueError) as exc:
            plog.get().error(
                "gameserver_allocate_rejected",
                reason=AGONES_REJECT_MARSHAL_FAILED,
                match_id=match_id,
                map_id=map_id,
                release_track=release_track,
                err=repr(exc),
            )
            raise errcode.PandoraError(
                errcode.ErrDSAllocationFailed, "agones: marshal request: %s", exc, cause=exc
            ) from exc

        req_url = (
            f"{self._api_server}/apis/allocation.agones.dev/v1/namespaces/{self._namespace}"
            f"/gameserverallocations"
        )

        # §11.3 判据 5「慢在哪」:GSA POST 是整条进入链上唯一要等 apiserver 的同步调用,
        # 每次尝试(canary → stable 回退是两次)各自计时,否则只能看到一个合并总时长。
        started_at = time.monotonic()
        selected_fleets = [s["matchLabels"][FLEET_LABEL_KEY] for s in selectors]

        resp = await self._do("POST", req_url, payload)
        if resp.error is not None:
            plog.get().warning(
                "gameserver_allocate_rejected",
                reason=AGONES_REJECT_TRANSPORT_FAILED,
                match_id=match_id,
                map_id=map_id,
                release_track=release_track,
                fleets=selected_fleets,
                elapsed_ms=_elapsed_ms(started_at),
                err=repr(resp.error),
                hint="apiserver 不可达/超时:本次 POST 结果未知,上游会保留 allocation_id 对账",
            )
            raise errcode.PandoraError(
                errcode.ErrDSAllocationFailed,
                "agones: allocate match %d: %s",
                match_id,
                resp.error,
                cause=resp.error,
            )
        if not _is_2xx(resp.status):
            plog.get().warning(
                "gameserver_allocate_rejected",
                reason=AGONES_REJECT_HTTP_ERROR,
                match_id=match_id,
                map_id=map_id,
                release_track=release_track,
                fleets=selected_fleets,
                http_status=resp.status,
                elapsed_ms=_elapsed_ms(started_at),
                body=truncate(resp.body, 256),
            )
            raise errcode.PandoraError(
                errcode.ErrDSAllocationFailed,
                "agones: allocate match %d http %d: %s",
                match_id,
                resp.status,
                truncate(resp.body, 256),
            )

        try:
            status = _gsa_status(resp.body)
        except AgonesError as exc:
            plog.get().warning(
                "gameserver_allocate_rejected",
                reason=AGONES_REJECT_DECODE_FAILED,
                match_id=match_id,
                release_track=release_track,
                http_status=resp.status,
                elapsed_ms=_elapsed_ms(started_at),
                err=repr(exc),
            )
            raise errcode.PandoraError(
                errcode.ErrDSAllocationFailed, "agones: decode response: %s", exc, cause=exc
            ) from exc

        state, gs_name, address, ports = status
        # state 只有 "Allocated" 才表示拿到了 GameServer;UnAllocated / Contention = 无空闲。
        if state != AGONES_STATE_ALLOCATED:
            # 容量不足是"进不去副本"的头号根因。带上 state 与候选 Fleet:
            # UnAllocated=真没空闲副本(该扩容),Contention=并发抢占(重试即可),
            # 两者处置截然不同。
            plog.get().warning(
                "gameserver_allocate_rejected",
                reason=AGONES_REJECT_NO_AVAILABLE,
                match_id=match_id,
                map_id=map_id,
                release_track=release_track,
                fleets=selected_fleets,
                state=state,
                elapsed_ms=_elapsed_ms(started_at),
                hint="UnAllocated=无空闲 GameServer(看 fleet 容量/扩容);Contention=并发抢占",
            )
            raise errcode.PandoraError(
                errcode.ErrDSNoAvailable,
                "agones: no gameserver for match %d (state=%r)",
                match_id,
                state,
            )
        if gs_name == "" or address == "" or not ports:
            plog.get().warning(
                "gameserver_allocate_rejected",
                reason=AGONES_REJECT_STATUS_INCOMPLETE,
                match_id=match_id,
                release_track=release_track,
                pod=gs_name,
                addr=address,
                ports=len(ports),
                elapsed_ms=_elapsed_ms(started_at),
            )
            raise errcode.PandoraError(
                errcode.ErrDSAllocationFailed,
                "agones: incomplete status for match %d: name=%r addr=%r ports=%d",
                match_id,
                gs_name,
                address,
                len(ports),
            )

        host = self._advertise_host or address
        addr = f"{host}:{ports[0]}"
        # R1:拿到 GameServer 是进入链的第一个不可逆推进。每局至多两条(canary 回退时),
        # 带 fleet 与 elapsed_ms 才答得了"分配到哪个 Fleet / Agones 这一步慢不慢"。
        plog.get().info(
            "gameserver_allocated",
            match_id=match_id,
            map_id=map_id,
            pod=gs_name,
            ds_addr=addr,
            release_track=release_track,
            fleets=selected_fleets,
            elapsed_ms=_elapsed_ms(started_at),
        )
        return gs_name, addr

    # ── 凭据投递 ─────────────────────────────────────────────────────────

    async def deliver_credential(
        self,
        allocation: AuthoritativeGameServerAllocation | None,
        annotations: dict[str, str],
    ) -> str:
        """用 UID + resourceVersion JSON Patch 投递 Redis pending 的镜像,返回新的 rv。

        对应 Go 的 `DeliverCredential`。

        ★ PATCH 的 HTTP 结果**从不**直接作为成功依据:无论 2xx 空 / 坏响应、409,
          还是 transport timeout,均再做一次严格 GET;只有 UID 未变且全部 annotation
          与期望精确相等才成功,绝不本地 fallback。
        """
        if (
            allocation is None
            or allocation.pod_name == ""
            or allocation.instance_uid == ""
            or allocation.resource_version == ""
            or not annotations
        ):
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "agones: incomplete credential delivery input"
            )
        for k, v in annotations.items():
            if k == "" or v == "":
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg,
                    "agones: credential annotation key/value must be non-empty",
                )
        ops: list[dict[str, Any]] = [
            {"op": "test", "path": "/metadata/uid", "value": allocation.instance_uid},
            {
                "op": "test",
                "path": "/metadata/resourceVersion",
                "value": allocation.resource_version,
            },
        ]
        if not allocation.annotations_present:
            # annotations 整体缺失时一次创建;RV test 保证不会覆盖并发新增的 annotations。
            ops.append({"op": "add", "path": "/metadata/annotations", "value": annotations})
        else:
            for key in sorted(annotations):
                ops.append(
                    {
                        "op": "add",
                        "path": "/metadata/annotations/" + escape_json_pointer(key),
                        "value": annotations[key],
                    }
                )
        payload = json.dumps(ops).encode("utf-8")
        gs_url = (
            f"{self._api_server}/apis/agones.dev/v1/namespaces/{self._namespace}"
            f"/gameservers/{allocation.pod_name}"
        )
        patch = await self._do("PATCH", gs_url, payload, "application/json-patch+json")

        # PATCH 结果未知时仍要做一次独立、有界的确认读:它只读 K8s 当前事实,不延长
        # 业务写。Go 那边用 plog.Detach(ctx) 把请求级 transport/取消剥掉;Python 侧
        # 没有请求 ctx 可剥,`_do` 自带 allocate_timeout 上界,而 asyncio 的取消是
        # 停机信号必须穿透(见模块级约定)。
        confirmed: GameServerResponse | None = None
        confirm_err: str | None = None
        try:
            confirmed = await self._get_game_server(allocation.pod_name)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 — 与 Go 的 confirmErr 同宽
            confirm_err = repr(exc)
        if confirm_err is None:
            confirm_err = confirm_credential_delivery(confirmed, allocation, annotations)
        if confirm_err is None and confirmed is not None:
            # R4 邻居项:凭据投递每局一次但会随轮换重试,成功侧 Debug;
            # 激活这个不可逆推进已由 biz 的 battle_ds_credential_activated 打 Info。
            plog.get().debug(
                "ds_credential_delivered",
                pod=allocation.pod_name,
                uid=allocation.instance_uid,
                allocation_id=allocation.allocation_id,
                patch_status=patch.status,
                annotations=len(annotations),
            )
            return confirmed.metadata.resource_version
        # 投递未被严格确认 = DS 拿不到回调凭据 = 该局心跳全部会被拒。这是
        # "DS 起来了但后端当它不存在"的直接上游,必须带 patch_status 与 confirm_err 分开定位。
        plog.get().warning(
            "ds_credential_delivery_rejected",
            reason=AGONES_REJECT_CREDENTIAL_UNCONFIRMED,
            pod=allocation.pod_name,
            uid=allocation.instance_uid,
            allocation_id=allocation.allocation_id,
            patch_status=patch.status,
            patch_err=repr(patch.error) if patch.error is not None else None,
            confirm_err=confirm_err,
            hint="凭据未落到 GameServer annotation;DS 心跳会被判未授权,分配将失败回收",
        )
        raise errcode.PandoraError(
            errcode.ErrDSAllocationFailed,
            "agones: credential PATCH not strictly confirmed: "
            "patch_status=%d patch_err=%s patch_body=%r confirm_err=%s",
            patch.status,
            patch.error,
            truncate(patch.body, 256),
            confirm_err,
        )

    # ── 严格 GET ─────────────────────────────────────────────────────────

    async def _get_game_server(self, pod_name: str) -> GameServerResponse:
        gs_url = (
            f"{self._api_server}/apis/agones.dev/v1/namespaces/{self._namespace}"
            f"/gameservers/{pod_name}"
        )
        resp = await self._do("GET", gs_url)
        if resp.error is not None:
            raise AgonesError(str(resp.error)) from resp.error
        if not _is_2xx(resp.status):
            raise AgonesError(
                f"GET gameserver http {resp.status}: {truncate(resp.body, 256)}"
            )
        return _parse_game_server(_load_json(resp.body, "gameserver"))

    async def _get_pod(self, pod_name: str) -> PodResponse:
        pod_url = f"{self._api_server}/api/v1/namespaces/{self._namespace}/pods/{pod_name}"
        resp = await self._do("GET", pod_url)
        if resp.error is not None:
            raise AgonesError(str(resp.error)) from resp.error
        if not _is_2xx(resp.status):
            raise AgonesError(f"GET pod http {resp.status}: {truncate(resp.body, 256)}")
        return _parse_pod(_load_json(resp.body, "pod"))

    async def resolve_expected_pod_uid(
        self, allocation: AuthoritativeGameServerAllocation | None
    ) -> str:
        """滚动升级期唯一允许的 pod_uid 回填。对应 Go 的 `ResolveExpectedPodUID`。

        两个 Kubernetes 对象都在**任何删除之前**读取,且每一项持久绑定都要复核。
        对象缺失或同名替换刻意**不**当成功 —— 那时旧的 Pod UID 已无法被证明。
        """
        if allocation is None:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "agones: pod UID preflight requires gameserver name, uid and allocation_id",
            )
        parsed = _parse_canonical_uuid_v4(allocation.allocation_id)
        if allocation.pod_name == "" or allocation.instance_uid == "" or parsed is None:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "agones: pod UID preflight requires gameserver name, uid and allocation_id",
            )
        try:
            gs = await self._get_game_server(allocation.pod_name)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrDSAllocationFailed,
                "agones: pod UID preflight gameserver GET failed: %s",
                exc,
                cause=exc,
            ) from exc
        if (
            gs.metadata.name != allocation.pod_name
            or gs.metadata.uid != allocation.instance_uid
            or gs.metadata.label(BATTLE_ALLOCATION_METADATA_KEY)
            != sanitize_label_value(allocation.allocation_id)
            or gs.metadata.ann(BATTLE_ALLOCATION_METADATA_KEY) != allocation.allocation_id
        ):
            plog.get().warning(
                "gameserver_allocate_rejected",
                reason=AGONES_REJECT_POD_UID_PREFLIGHT,
                pod=gs.metadata.name,
                uid=gs.metadata.uid,
                allocation_id=allocation.allocation_id,
            )
            raise errcode.PandoraError(
                errcode.ErrDSAllocationFailed,
                "agones: pod UID preflight gameserver identity/binding changed: "
                "pod=%r uid=%r allocation_id=%r",
                gs.metadata.name,
                gs.metadata.uid,
                allocation.allocation_id,
            )
        try:
            pod = await self._get_pod(allocation.pod_name)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrDSAllocationFailed,
                "agones: pod UID preflight pod GET failed: %s",
                exc,
                cause=exc,
            ) from exc
        if not pod_owned_by_game_server(pod, allocation.pod_name, allocation.instance_uid):
            raise errcode.PandoraError(
                errcode.ErrDSAllocationFailed,
                "agones: pod UID preflight owner/identity changed: "
                "pod=%r pod_uid=%r gameserver_uid=%r",
                allocation.pod_name,
                pod.uid,
                allocation.instance_uid,
            )
        return pod.uid

    # ── 判死探测 / 回收 ──────────────────────────────────────────────────

    async def probe_expected_instance_gone(
        self, pod_name: str, instance_uid: str, pod_uid: str
    ) -> bool:
        """只读探测 exact 实例是否已可证死亡。对应 Go 的 `ProbeExpectedInstanceGone`。

          - GameServer NotFound / UID 已换代,且(有持久 pod_uid 时)关联 Pod 同样
            NotFound / UID 已换代 → `True`:物理实例已确认消失;
          - GameServer 同 UID 且 state=Unhealthy → `True`:Agones 依据 SDK health ping
            (DS 侧独立线程 pinger)断流判死,该实例不可能再服务本局;
          - 同 UID 且非 Unhealthy → `False`:实例存活(可能仍在冷加载);
          - 任何读失败 / 不确定 → **抛异常**:调用方必须回退时间界,不得据此回收。

        零写副作用;判弃权威仍是 Redis 事务(AbandonIfStale 的 WATCH 单赢家)。
        """
        if pod_name == "" or instance_uid == "":
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "agones: instance probe requires gameserver name and uid"
            )
        gs_url = (
            f"{self._api_server}/apis/agones.dev/v1/namespaces/{self._namespace}"
            f"/gameservers/{pod_name}"
        )
        resp = await self._do("GET", gs_url)
        if resp.error is not None:
            raise AgonesError(str(resp.error)) from resp.error
        if resp.status == _HTTP_NOT_FOUND:
            pass  # GameServer 已消失,继续向下做 Pod 双确认。
        elif _is_2xx(resp.status):
            gs = _parse_game_server(_load_json(resp.body, "gameserver probe"))
            if gs.metadata.uid == "":
                raise AgonesError("gameserver probe missing metadata.uid")
            if gs.metadata.uid == instance_uid:
                return gs.state == AGONES_STATE_UNHEALTHY
            # 同名对象已是新实例:旧 UID 物理消失,继续 Pod 双确认。
        else:
            raise AgonesError(
                f"GET gameserver probe http {resp.status}: {truncate(resp.body, 128)}"
            )
        if pod_uid == "":
            # 无持久 Pod UID 可对账时只认 GameServer 消失(Model B 分配在 finalize 即
            # 持久化 pod_uid,该回退只覆盖历史残留记录)。
            return True
        pod_url = f"{self._api_server}/api/v1/namespaces/{self._namespace}/pods/{pod_name}"
        return await self._resource_uid_gone(pod_url, pod_uid)

    async def _probe_exact_resource(
        self, resource_url: str, expected_uid: str
    ) -> ExactResourceProbe:
        """对应 Go 的 `probeExactResource`。"""
        resp = await self._do("GET", resource_url)
        if resp.error is not None:
            raise AgonesError(str(resp.error)) from resp.error
        if resp.status == _HTTP_NOT_FOUND:
            return ExactResourceProbe(gone=True)
        if not _is_2xx(resp.status):
            raise AgonesError(
                f"GET exact resource http {resp.status}: {truncate(resp.body, 128)}"
            )
        obj = _load_json(resp.body, "exact resource identity")
        if not isinstance(obj, dict):
            raise AgonesError("agones: decode exact resource identity: payload is not an object")
        md = obj.get("metadata") or {}
        if not isinstance(md, dict):
            raise AgonesError("agones: decode exact resource identity: metadata is not an object")
        uid = _as_str(md, "uid")
        if uid == "":
            raise AgonesError("exact resource missing metadata.uid")
        if uid != expected_uid:
            return ExactResourceProbe(gone=True)
        return ExactResourceProbe(deleting=_as_str(md, "deletionTimestamp") != "")

    async def _resource_uid_gone(self, resource_url: str, expected_uid: str) -> bool:
        """保留「物理消失才算 gone」语义(deleting 不算)。对应 Go 的 `resourceUIDGone`。"""
        probe = await self._probe_exact_resource(resource_url, expected_uid)
        return probe.gone

    async def release_expected(self, allocation: AuthoritativeGameServerAllocation | None) -> None:
        """只删除 UID 仍等于本次分配实例的 GameServer。对应 Go 的 `ReleaseExpected`。

        DELETE 2xx 只是 deletion request accepted,不是物理消失证明;本方法会继续轮询
        exact GameServer UID 及分配时捕获的关联 Pod UID,只有两者都 NotFound / UID
        changed 才正常返回。

        同 UID 已带 deletionTimestamp(删除已受理、Pod 处终止宽限)时**跳过重复 DELETE**,
        并快速抛 `ReleaseDeletionPendingError` 而非空耗轮询 —— 宽限内对象不可能物理消失,
        空转只会占住 sweep 队头(INC-20260727-001 复审 P1-2)。
        """
        if allocation is None or (
            allocation.instance_uid == "" and allocation.allocation_id == ""
        ):
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "agones: expected release requires uid or allocation_id"
            )
        if allocation.instance_uid == "":
            await self._release_by_allocation_id(allocation)
            return
        if allocation.pod_name == "":
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "agones: UID release requires gameserver name"
            )
        if allocation.pod_uid == "":
            # 绝不能只在进程内存里捕获 Pod UID 就发 DELETE:apiserver 已受理删除之后
            # 崩溃会永久丢失该 UID,重启后的对账者再也无法证明"那个物理 Pod 消失了"。
            # 新分配在任何外部删除之前就持久化 pod_uid;历史记录必须由 release preflight
            # 排空 / 迁移。
            raise errcode.PandoraError(
                errcode.ErrDSAllocationFailed,
                "agones: durable associated pod UID required before exact release: "
                "pod=%s gameserver_uid=%s",
                allocation.pod_name,
                allocation.instance_uid,
            )
        gs_url = (
            f"{self._api_server}/apis/agones.dev/v1/namespaces/{self._namespace}"
            f"/gameservers/{allocation.pod_name}"
        )
        # 删除前先读 exact 状态:已消失或已带 deletionTimestamp 都无需再发 DELETE
        # (重复 DELETE 是纯噪声;实测每实例 7 次重复,INC-20260727-001 复审 P1-2)。
        try:
            pre_probe = await self._probe_exact_resource(gs_url, allocation.instance_uid)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            plog.get().warning(
                "gameserver_release_rejected",
                reason=AGONES_REJECT_RELEASE_PROBE_FAILED,
                pod=allocation.pod_name,
                uid=allocation.instance_uid,
                err=repr(exc),
            )
            raise errcode.PandoraError(
                errcode.ErrDSAllocationFailed,
                "agones: exact release pre-probe failed: pod=%s gs_uid=%s err=%s",
                allocation.pod_name,
                allocation.instance_uid,
                exc,
                cause=exc,
            ) from exc

        delete_status = 0
        delete_err: Exception | None = None
        delete_body = b""
        if not pre_probe.gone and not pre_probe.deleting:
            body = json.dumps(
                {
                    "apiVersion": "v1",
                    "kind": "DeleteOptions",
                    "preconditions": {"uid": allocation.instance_uid},
                }
            ).encode("utf-8")
            resp = await self._do("DELETE", gs_url, body)
            delete_status, delete_err, delete_body = resp.status, resp.error, resp.body

        try:
            await self._wait_expected_instance_gone(allocation)
        except ReleaseDeletionPendingError as exc:
            # 保持哨兵可被 `except ReleaseDeletionPendingError` 识别:调用方按分配身份
            # 退避,不重复 DELETE。
            raise ReleaseDeletionPendingError(
                f"agones: exact release awaiting termination grace: pod={allocation.pod_name} "
                f"gs_uid={allocation.instance_uid} pod_uid={allocation.pod_uid}: {exc}"
            ) from exc
        except asyncio.CancelledError:
            raise
        except Exception as confirm_err:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrDSAllocationFailed,
                "agones: exact release not physically confirmed: pod=%s gs_uid=%s pod_uid=%s "
                "delete_status=%d delete_err=%s delete_body=%r confirm_err=%s",
                allocation.pod_name,
                allocation.instance_uid,
                allocation.pod_uid,
                delete_status,
                delete_err,
                truncate(delete_body, 128),
                confirm_err,
                cause=confirm_err,
            ) from confirm_err

    async def _release_by_allocation_id(
        self, allocation: AuthoritativeGameServerAllocation
    ) -> None:
        """POST 已选中但 UID GET 不确定时的按 label 精确回收。对应 Go 的同名分支。

        不能按名字删:allocation_id 是本次 GSA 写入选中对象的唯一 UUID label,同名重建
        的新对象不会带旧 allocation_id,因此旧 cleanup 不会误杀新实例。
        """
        selector = BATTLE_ALLOCATION_METADATA_KEY + "=" + sanitize_label_value(
            allocation.allocation_id
        )
        delete_body = json.dumps({"apiVersion": "v1", "kind": "DeleteOptions"}).encode("utf-8")
        collection_url = (
            f"{self._api_server}/apis/agones.dev/v1/namespaces/{self._namespace}"
            f"/gameservers?labelSelector={urllib.parse.quote_plus(selector)}"
        )
        # LIST 先行(复审必修):对象已全部处于删除宽限时直接返回 pending,不重复
        # DeleteCollection。空集合或含存活对象时才发 DeleteCollection —— 空集合仍保留
        # DeleteCollection+后置 LIST 的 timeout-late-apply 防线(此前一次 DeleteCollection
        # 可能已被受理但响应丢失,幂等再发无害且能捕获迟到 apply 出现的新对象)。
        pre = await self._do("GET", collection_url)
        if pre.error is None and _is_2xx(pre.status):
            pre_items = _list_items_or_none(pre.body)
            if pre_items:
                if all(_deletion_timestamp(item) != "" for item in pre_items):
                    raise ReleaseDeletionPendingError(
                        "agones: allocation-id release awaiting termination grace (pre-list): "
                        f"allocation_id={allocation.allocation_id} items={len(pre_items)}"
                    )
        delete_resp = await self._do("DELETE", collection_url, delete_body)
        # DeleteCollection 的响应/超时同样不构成完成证据;严格 LIST 确认该唯一 label
        # 已无对象。timeout-but-applied 可幂等成功,2xx 但仍有对象则保留 Redis claim。
        list_resp = await self._do("GET", collection_url)
        if list_resp.error is None and _is_2xx(list_resp.status):
            items = _list_items_or_none(list_resp.body)
            if items is not None:
                if len(items) == 0:
                    return
                if all(_deletion_timestamp(item) != "" for item in items):
                    # 全部对象删除已受理:等终止宽限,调用方退避后重试确认,不算失败。
                    raise ReleaseDeletionPendingError(
                        "agones: allocation-id release awaiting termination grace: "
                        f"allocation_id={allocation.allocation_id} items={len(items)}"
                    )
        plog.get().warning(
            "gameserver_release_rejected",
            reason=AGONES_REJECT_RELEASE_NOT_CONFIRMED,
            allocation_id=allocation.allocation_id,
            http_status=delete_resp.status,
        )
        raise errcode.PandoraError(
            errcode.ErrDSAllocationFailed,
            "agones: allocation-id release not confirmed: allocation_id=%s delete_status=%d "
            "delete_err=%s delete_body=%r list_status=%d list_err=%s list_body=%r",
            allocation.allocation_id,
            delete_resp.status,
            delete_resp.error,
            truncate(delete_resp.body, 128),
            list_resp.status,
            list_resp.error,
            truncate(list_resp.body, 128),
        )

    async def _wait_expected_instance_gone(
        self, allocation: AuthoritativeGameServerAllocation
    ) -> None:
        """轮询确认 exact GameServer 与 Pod 都物理消失。对应 Go 的 `waitExpectedInstanceGone`。

        ★ 这是 §16.10 里**允许**的那一类"到期兜底":每一轮都重新向 apiserver 查权威,
          到期后抛错交调用方退避,**从不**"到期即假设已删除"。
        """
        wait = self._allocate_timeout
        if wait <= 0:
            wait = dconf.DEFAULT_AGONES_ALLOCATE_TIMEOUT.total_seconds()
        loop = asyncio.get_running_loop()
        deadline = loop.time() + wait
        gs_url = (
            f"{self._api_server}/apis/agones.dev/v1/namespaces/{self._namespace}"
            f"/gameservers/{allocation.pod_name}"
        )
        pod_url = (
            f"{self._api_server}/api/v1/namespaces/{self._namespace}/pods/{allocation.pod_name}"
        )
        last_gs = ExactResourceProbe()
        last_pod = ExactResourceProbe()
        last_gs_err: str | None = None
        last_pod_err: str | None = None
        while True:
            last_gs_err = None
            last_pod_err = None
            try:
                last_gs = await self._probe_exact_resource(gs_url, allocation.instance_uid)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                last_gs, last_gs_err = ExactResourceProbe(), repr(exc)
            try:
                last_pod = await self._probe_exact_resource(pod_url, allocation.pod_uid)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                last_pod, last_pod_err = ExactResourceProbe(), repr(exc)
            if last_gs_err is None and last_pod_err is None:
                if last_gs.gone and last_pod.gone:
                    return
                # 任一对象带 deletionTimestamp:删除已受理,物理消失要等终止宽限
                # (默认 30s,远超本确认窗口),继续轮询只是空耗 —— 立即抛 pending
                # 哨兵交调用方退避。
                if last_gs.deleting or last_pod.deleting:
                    raise ReleaseDeletionPendingError(
                        f"gs_gone={last_gs.gone} gs_deleting={last_gs.deleting} "
                        f"pod_gone={last_pod.gone} pod_deleting={last_pod.deleting}"
                    )
            if loop.time() >= deadline:
                raise AgonesError(
                    f"timeout waiting exact objects gone: gs={last_gs} gs_err={last_gs_err} "
                    f"pod={last_pod} pod_err={last_pod_err}"
                )
            await asyncio.sleep(0.05)

    async def release(self, pod_name: str) -> None:
        """DELETE 该 GameServer(Fleet 自动补新);404 视作已释放(幂等)。对应 Go 的 `Release`。"""
        if pod_name == "":
            return
        gs_url = (
            f"{self._api_server}/apis/agones.dev/v1/namespaces/{self._namespace}"
            f"/gameservers/{pod_name}"
        )
        resp = await self._do("DELETE", gs_url)
        if resp.error is not None:
            raise errcode.PandoraError(
                errcode.ErrDSAllocationFailed,
                "agones: release %s: %s",
                pod_name,
                resp.error,
                cause=resp.error,
            )
        if resp.status == _HTTP_NOT_FOUND:
            return  # 已不存在 = 已释放,幂等
        if not _is_2xx(resp.status):
            plog.get().warning(
                "gameserver_release_rejected",
                reason=AGONES_REJECT_RELEASE_HTTP_ERROR,
                pod=pod_name,
                http_status=resp.status,
            )
            raise errcode.PandoraError(
                errcode.ErrDSAllocationFailed,
                "agones: release %s http %d: %s",
                pod_name,
                resp.status,
                truncate(resp.body, 256),
            )

    # ── 孤儿 Allocated GameServer 对账回收(2026-08-03)───────────────────
    #
    # 背景:Agones 生命周期**不回收** Allocated GameServer。若一台 GS 处于 Allocated
    # 却在权威存储里没有任何分配记录引用(记录已释放但外部删除失败/响应丢失、手工 GSA、
    # 历史残留),它会永久占位锁死 Fleet 容量;而人工判断"无人"必须翻日志,日志窗口/
    # 轮转/级别静默三重失真已两次导致误删载人 DS。本节提供机械化的只读列举 + exact
    # 复核删除,由 biz 层 sweep 的对账清扫消费(§9.21:禁止删除仍承载玩家的 Allocated DS)。

    async def list_allocated_game_servers(self) -> list[AllocatedGameServerInfo]:
        """列出全部受管 Fleet 下处于 Allocated 状态的 GameServer。对应 Go 的
        `ListAllocatedGameServers`。

        ★ 任何错误都整轮失败(fail-closed):对账清扫拿不到完整清单时必须什么都不删,
          部分清单会把仍被引用的 GS 误判为孤儿。
        """
        fleets = self.watched_fleets()
        if not fleets:
            return []
        selector = FLEET_LABEL_KEY + " in (" + ",".join(fleets) + ")"
        # 分页 LIST:Allocated 只能在客户端过滤(CRD 的 status.state 未建索引,
        # fieldSelector 不可用),故拉回的是受管 Fleet 下**全部** GameServer;
        # 不分页时 Fleet 一大就会撞上单响应读上限。逐页取直到 metadata.continue 为空。
        out: list[AllocatedGameServerInfo] = []
        continue_token = ""
        page = 0
        while True:
            list_url = (
                f"{self._api_server}/apis/agones.dev/v1/namespaces/{self._namespace}"
                f"/gameservers?labelSelector={urllib.parse.quote_plus(selector)}"
                f"&limit={AGONES_LIST_PAGE_SIZE}"
            )
            if continue_token != "":
                list_url += "&continue=" + urllib.parse.quote_plus(continue_token)
            resp = await self._do("GET", list_url)
            if resp.error is not None:
                raise AgonesError(
                    f"agones: list allocated gameservers (page {page}): {resp.error}"
                ) from resp.error
            if not _is_2xx(resp.status):
                raise AgonesError(
                    f"agones: list allocated gameservers (page {page}) http {resp.status}: "
                    f"{truncate(resp.body, 256)}"
                )
            payload = _load_json(resp.body, f"allocated gameserver list (page {page})")
            if not isinstance(payload, dict):
                raise AgonesError(
                    f"agones: decode allocated gameserver list (page {page}): "
                    "payload is not an object"
                )
            items = payload.get("items") or []
            if not isinstance(items, list):
                raise AgonesError(
                    f"agones: decode allocated gameserver list (page {page}): "
                    "items is not an array"
                )
            for raw in items:
                item = _parse_game_server(raw)
                if item.state != AGONES_STATE_ALLOCATED:
                    continue
                out.append(
                    AllocatedGameServerInfo(
                        name=item.metadata.name,
                        uid=item.metadata.uid,
                        fleet=item.metadata.label(FLEET_LABEL_KEY),
                        allocation_id=item.metadata.label(BATTLE_ALLOCATION_METADATA_KEY),
                        deleting=item.metadata.deletion_timestamp != "",
                    )
                )
            md = payload.get("metadata") or {}
            if not isinstance(md, dict):
                raise AgonesError(
                    f"agones: decode allocated gameserver list (page {page}): "
                    "metadata is not an object"
                )
            continue_token = _as_str(md, "continue")
            if continue_token == "":
                return out
            page += 1

    async def delete_allocated_game_server_exact(
        self, name: str, uid: str, expected_allocation_id: str
    ) -> bool:
        """回收一台已被 biz 层判定为孤儿的 Allocated GameServer。对应 Go 的
        `DeleteAllocatedGameServerExact`。

        删除前在服务端做一次 exact 复核(GET 最新对象),四项任一不符即返回 `False`
        让调用方作废候选重新观察,绝不带着过期观察结论删除:

          - UID 不符:同名对象已是新实例;
          - 已带 deletionTimestamp:删除已受理;
          - 状态离开 Allocated:候选前提不成立;
          - allocation-id label 与候选观察值不符:GET 前发生过新的 GSA 写入。

        ★ DELETE 携带 **UID + resourceVersion 双 precondition**:GET→DELETE 窗口内
          对象的**任何**变更(包括一次新的 GSA 分配,它必然 PATCH metadata 抬高
          resourceVersion)都会让 precondition 失败(409),从机制上排除"复核后瞬间
          被分配、随即被删"的竞态。404/409 都按候选失效返回 `False`;只有确认 2xx
          才返回 `True`。
        """
        if name == "" or uid == "":
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "agones: orphan reclaim requires gameserver name and uid"
            )
        gs_url = (
            f"{self._api_server}/apis/agones.dev/v1/namespaces/{self._namespace}"
            f"/gameservers/{name}"
        )
        resp = await self._do("GET", gs_url)
        if resp.error is not None:
            raise AgonesError(f"agones: orphan reclaim get {name}: {resp.error}") from resp.error
        if resp.status == _HTTP_NOT_FOUND:
            return False  # 已消失,无需动作
        if not _is_2xx(resp.status):
            raise AgonesError(
                f"agones: orphan reclaim get {name} http {resp.status}: "
                f"{truncate(resp.body, 256)}"
            )
        gs = _parse_game_server(_load_json(resp.body, f"orphan reclaim {name}"))
        if (
            gs.metadata.uid != uid
            or gs.metadata.deletion_timestamp != ""
            or gs.state != AGONES_STATE_ALLOCATED
            or gs.metadata.label(BATTLE_ALLOCATION_METADATA_KEY) != expected_allocation_id
        ):
            return False
        body = json.dumps(
            {
                "apiVersion": "v1",
                "kind": "DeleteOptions",
                "preconditions": {
                    "uid": uid,
                    "resourceVersion": gs.metadata.resource_version,
                },
            }
        ).encode("utf-8")
        delete_resp = await self._do("DELETE", gs_url, body)
        if delete_resp.error is not None:
            raise AgonesError(
                f"agones: orphan reclaim delete {name}: {delete_resp.error}"
            ) from delete_resp.error
        if delete_resp.status in (_HTTP_NOT_FOUND, _HTTP_CONFLICT):
            # 404=对象已消失;409=precondition 失败(GET→DELETE 窗口内对象变过)。
            # 都不是本次删除完成,按候选失效处理。
            return False
        if not _is_2xx(delete_resp.status):
            raise AgonesError(
                f"agones: orphan reclaim delete {name} http {delete_resp.status}: "
                f"{truncate(delete_resp.body, 256)}"
            )
        return True

    # ── Fleet 容量巡检(K8s 快上限预警,2026-07-10)──────────────────────

    def watched_fleets(self) -> list[str]:
        """容量巡检要盯的 Fleet 集合。对应 Go 的 `WatchedFleets`。

        通用池 fleet_name + canary_fleet_name + 全部 map_fleets 专属预热池;去重,
        通用池在前,专属池按名字典序 —— 保证顺序稳定(Go 的 map 遍历无序,靠 sort 定序)。
        """
        out: list[str] = []
        seen: set[str] = set()

        def add(name: str) -> None:
            if name != "" and name not in seen:
                seen.add(name)
                out.append(name)

        add(self._fleet_name)
        add(self._canary_fleet_name)
        dedicated: list[str] = []
        for route in self._map_fleets.values():
            for name in (route.stable, route.canary):
                if name != "" and name not in seen:
                    seen.add(name)
                    dedicated.append(name)
        dedicated.sort()
        return out + dedicated

    def canary_fleets(self) -> set[str]:
        """「只属金丝雀轨」的 Fleet 名集合。对应 Go 的 `canaryFleets`。

        ★ 同名兜底:若配置把 stable 与 canary 指到同一个 Fleet(误配),该名字按
          **stable** 处理(与 `watched_fleets` 里 stable 先入的去重顺序一致),
          不享受 canary 的 desired==0 静默,宁可多报也不静音真实容量问题。
        """
        stable: set[str] = set()
        if self._fleet_name != "":
            stable.add(self._fleet_name)
        for route in self._map_fleets.values():
            if route.stable != "":
                stable.add(route.stable)
        canary: set[str] = set()

        def add(name: str) -> None:
            if name != "" and name not in stable:
                canary.add(name)

        add(self._canary_fleet_name)
        for route in self._map_fleets.values():
            add(route.canary)
        return canary

    async def list_fleet_capacities(self) -> tuple[list[FleetCapacity], AgonesError | None]:
        """GET 每个受管 Fleet 的 spec/status,返回容量快照。对应 Go 的 `ListFleetCapacities`。

        ★ Go 的签名是 `([]FleetCapacity, error)` 且**两者都有意义**(单个 Fleet 查询
          失败不影响其余,部分成功也返回,错误经 `errors.Join` 汇总供上层打日志)。
          Python 用异常传播会把已经拿到的快照整片丢掉 —— 那正好是 §9.22 反过来的
          错误:把"部分已知"退化成"完全未知"。故这里保留双返回值。
        """
        out: list[FleetCapacity] = []
        errs: list[str] = []
        canary_track = self.canary_fleets()
        for fleet in self.watched_fleets():
            fleet_url = (
                f"{self._api_server}/apis/agones.dev/v1/namespaces/{self._namespace}"
                f"/fleets/{fleet}"
            )
            resp = await self._do("GET", fleet_url)
            if resp.error is not None:
                errs.append(f"agones: get fleet {fleet}: {resp.error}")
                continue
            if not _is_2xx(resp.status):
                errs.append(
                    f"agones: get fleet {fleet} http {resp.status}: {truncate(resp.body, 256)}"
                )
                continue
            try:
                payload = _load_json(resp.body, f"fleet {fleet}")
                capacity = _parse_fleet_capacity(fleet, payload)
            except AgonesError as exc:
                errs.append(f"agones: decode fleet {fleet}: {exc}")
                continue
            capacity.canary = fleet in canary_track
            out.append(capacity)
        joined = AgonesError("\n".join(errs)) if errs else None
        return out, joined

    # ── 传输 ─────────────────────────────────────────────────────────────

    async def _do(
        self,
        method: str,
        req_url: str,
        body: bytes | None = None,
        content_type: str = "application/json",
    ) -> KubeResponse:
        """发一次带鉴权的 REST 请求,返回 `(body, status, transport_error)`。

        对应 Go 的 `do` / `doWithContentType`。传输错误不抛(理由见 `KubeResponse`)。
        """
        headers = {"Accept": "application/json"}
        if body is not None:
            headers["Content-Type"] = content_type
        # 每次请求重读 token(容忍 in-cluster 投影 token 轮转);"-" 或空 → 不带。
        if self._token_path != "" and self._token_path != "-":
            try:
                raw = pathlib.Path(self._token_path).read_text(encoding="utf-8")
            except OSError:
                raw = None  # Go: `if tok, terr := os.ReadFile(...); terr == nil`
            if raw is not None:
                headers["Authorization"] = "Bearer " + raw.strip()
        try:
            resp = await self._client.request(
                method, req_url, content=body, headers=headers, timeout=self._allocate_timeout
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 — 与 Go 的 transportErr 同宽
            return KubeResponse(b"", 0, exc)
        # 读上限必须**可检测**:裸截断会让调用方只看到形如 "unexpected end of JSON
        # input" 的解码错误,永远查不到真因是清单太大。超限时显式报错并指明修法(分页),
        # 绝不把半截响应当完整结果交出去。
        if len(resp.content) > AGONES_MAX_RESPONSE_BYTES:
            return KubeResponse(
                b"",
                resp.status_code,
                AgonesError(
                    f"agones: response body exceeds {AGONES_MAX_RESPONSE_BYTES} bytes limit "
                    f"(url={req_url});单次响应过大,需改用分页(limit+continue)而不是提高全局上限"
                ),
            )
        return KubeResponse(resp.content, resp.status_code, None)


# ── 响应解析辅助 ────────────────────────────────────────────────────────────


def _load_json(body: bytes, what: str) -> Any:
    try:
        return json.loads(body)
    except ValueError as exc:
        raise AgonesError(f"agones: decode {what}: {exc}") from exc


def _elapsed_ms(started_at: float) -> int:
    """对应 Go 的 `time.Since(startedAt).Milliseconds()`(向零截断)。"""
    return int((time.monotonic() - started_at) * 1000)


def _gsa_status(body: bytes) -> tuple[str, str, str, list[int]]:
    """解析 GameServerAllocation 响应的 status。返回 `(state, gameServerName, address, ports)`。"""
    payload = _load_json(body, "gameserverallocation response")
    if not isinstance(payload, dict):
        raise AgonesError("agones: gameserverallocation response is not an object")
    st = payload.get("status") or {}
    if not isinstance(st, dict):
        raise AgonesError("agones: gameserverallocation status is not an object")
    ports_raw = st.get("ports") or []
    if not isinstance(ports_raw, list):
        raise AgonesError("agones: gameserverallocation status.ports is not an array")
    ports: list[int] = []
    for p in ports_raw:
        if not isinstance(p, dict):
            raise AgonesError("agones: gameserverallocation status.ports item is not an object")
        port = p.get("port", 0)
        if not isinstance(port, int) or isinstance(port, bool):
            raise AgonesError("agones: gameserverallocation port must be an integer")
        ports.append(port)
    return (
        _as_str(st, "state"),
        _as_str(st, "gameServerName"),
        _as_str(st, "address"),
        ports,
    )


def _list_items(body: bytes) -> list[Any]:
    """解析 k8s list 响应的 `items`;缺失当空数组(Go 的 nil slice)。"""
    payload = _load_json(body, "gameserver list")
    if not isinstance(payload, dict):
        raise AgonesError("agones: gameserver list is not an object")
    items = payload.get("items")
    if items is None:
        return []
    if not isinstance(items, list):
        raise AgonesError("agones: gameserver list items is not an array")
    return items


def _list_items_or_none(body: bytes) -> list[Any] | None:
    """`_list_items` 的宽容版:解码失败返回 `None`。

    对应 Go 里 `if uerr := json.Unmarshal(...); uerr == nil { ... }` 那几处 ——
    解码不出来时**不做任何结论**,继续往下走到"未确认"分支,而不是抛出去。
    """
    try:
        return _list_items(body)
    except AgonesError:
        return None


def _deletion_timestamp(raw: Any) -> str:
    if not isinstance(raw, dict):
        return ""
    md = raw.get("metadata") or {}
    if not isinstance(md, dict):
        return ""
    value = md.get("deletionTimestamp")
    return value if isinstance(value, str) else ""


def _parse_fleet_capacity(fleet: str, payload: Any) -> FleetCapacity:
    """解析 Fleet 的 spec.replicas + status 三项。对应 Go 的 `fleetStatusResponse`。

    ★ `spec.replicas` 用 `desired_known` 显式区分「解码到且为 0」与「没解码到」——
      Go 那边用 `*uint32` 指针表达同一件事。让解码失败静默取零值,正是 §9.22 禁止的
      「把 UNKNOWN 冒充成确定值」。
    """
    if not isinstance(payload, dict):
        raise AgonesError(f"fleet {fleet} payload is not an object")
    spec = payload.get("spec") or {}
    status = payload.get("status") or {}
    if not isinstance(spec, dict) or not isinstance(status, dict):
        raise AgonesError(f"fleet {fleet} spec/status is not an object")

    def u32(container: dict[str, Any], key: str) -> int:
        value = container.get(key, 0)
        if value is None:
            return 0
        if not isinstance(value, int) or isinstance(value, bool) or not 0 <= value <= _UINT32_MAX:
            raise AgonesError(f"fleet {fleet} field {key!r} is not a uint32")
        return value

    capacity = FleetCapacity(
        fleet=fleet,
        replicas=u32(status, "replicas"),
        ready=u32(status, "readyReplicas"),
        allocated=u32(status, "allocatedReplicas"),
    )
    if spec.get("replicas") is not None:
        capacity.desired = u32(spec, "replicas")
        capacity.desired_known = True
    return capacity
