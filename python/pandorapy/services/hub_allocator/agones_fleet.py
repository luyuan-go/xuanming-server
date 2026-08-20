"""真 Agones `HubFleetProvider` —— 对应 Go 侧
`services/battle/hub_allocator/internal/biz/agones_fleet.go`。

与战斗 DS 的模型不同:大厅 Hub DS 是「常驻分片」而非「按需分配」。Hub DS GameServer
持续以 Ready 状态运行,hub_allocator 自己在 Redis 里维护各分片的 player_count 做容量
判定(不走 Agones GameServerAllocation)。因此本 provider 的职责是「发现拓扑」——
LIST Fleet 下的 GameServer(按 region 标签过滤),把可承载玩家的实例映射成
`ShardCandidate`,供 biz 层 lazy-seed 到 Redis。

── 为什么直接打 k8s REST,而不是用 `kubernetes` / `kubernetes_asyncio` ──────────
Go 侧刻意用标准库 `net/http` 直连 apiserver,不引入 agones/client-go 重依赖,理由是
保持 go.mod 干净、本地可编译可单测;Agones API 与 k8s provider 无关(ACK / 自建 /
minikube 一致),故 provider-agnostic。Python 侧同理:本文件只用到 4 个 REST 端点
(list/get gameserver、get pod、get/patch fleet),换成官方 client 会为此拖进一整套
model 代码生成 + 版本矩阵,而条件 PATCH 的 RFC-6902 语义还是得自己拼。
所以走 `httpx.AsyncClient`(已在 `pyproject.toml` 显式声明),额外好处是测试可以直接
注入 `httpx.MockTransport` 当假 apiserver,不需要真集群、也不需要 monkeypatch socket。

── 本文件最关键的一处不能抄错:条件 PATCH 是 **uid + resourceVersion 双 CAS** ────
Model B 的 `_patch_credential_annotation` 用 RFC-6902 的两条 `test` 操作:

    {"op":"test","path":"/metadata/uid",             "value": <uid>}
    {"op":"test","path":"/metadata/resourceVersion", "value": <rv>}

只 test uid **不够**:uid 只保证「还是同一个 GameServer 对象」,不保证「对象自我读取
以来没被别人改过」。少了 rv 这条,两个 hub_allocator 副本并发投递时,后到的低代际
bundle 会整片覆盖先到的高代际 bundle —— annotation 里是旧 gen、Redis 里是新 gen,
DS 拿着一张永远激活不了的 pending 令牌,而两边日志都是 2xx 全绿。
只 test rv 也不够:同名 GameServer 被删后重建,rv 会从新对象重新计数,旧实例的令牌
可能被投递给新实例(§9.22 的 exact 实例绑定被打穿)。两条缺一不可。
"""

from __future__ import annotations

import asyncio
import dataclasses
import datetime as _dt
import hashlib
import json
import pathlib
import ssl
import time
import urllib.parse
from typing import Any, Awaitable, Callable, Protocol, runtime_checkable

import httpx

from pandora.hub.v1 import allocator_pb2 as hubv1
from pandorapy import log as plog
from pandorapy import releasetrack
from pandorapy.protoenum import enum_name
from pandorapy.services.hub_allocator import conf as hconf
from pandorapy.services.hub_allocator.fleet import HubInstanceObservation, ShardCandidate

# ── GameServer 标签 / annotation key(与 ds_allocator 保持一致)───────────────

#: Agones Fleet 给其 GameServer 打的标签 key(selector 用)。
FLEET_LABEL_KEY = "agones.dev/fleet"
#: Pandora 给 Hub DS GameServer 打的分区标签(按 region 过滤分片)。
REGION_LABEL_KEY = "pandora.dev/region"
#: 可选:Hub DS 显式声明的稳定 shard_id(缺省则按 pod 名哈希派生)。
SHARD_ID_LABEL_KEY = "pandora.dev/shard-id"
#: 可选:单分片人数上限(缺省用 hub.default_capacity)。
CAPACITY_LABEL_KEY = "pandora.dev/capacity"
#: 同时用作精确 selector 与实际轨 annotation 审计字段。
RELEASE_TRACK_METADATA_KEY = "pandora.dev/release-track"

#: 「可承载玩家」的 GameServer 状态集合。Hub DS 常驻 Ready;运维若对其做过 Allocation
#: 保护(防缩容)则为 Allocated;Reserved 也短暂可用。其余(Shutdown / Error /
#: Unhealthy / Scheduled 等)排除。
HUB_READY_STATES = frozenset({"Ready", "Allocated", "Reserved"})

#: 下发 DS 回调令牌的 GameServer annotation key(与 ds_allocator 一致)。
DS_TOKEN_ANNOTATION_KEY = "pandora.dev/ds-token"
#: 令牌过期时刻(UnixMilli),供续期判定,避免解 JWT。
DS_TOKEN_EXP_ANNOTATION_KEY = "pandora.dev/ds-token-exp-ms"
#: 令牌代际(Redis INCR 单调值),供拓扑对账写入分片记录做精确代际比较。
DS_TOKEN_GEN_ANNOTATION_KEY = "pandora.dev/ds-token-gen"
DS_TOKEN_JTI_ANNOTATION_KEY = "pandora.dev/ds-token-jti"
DS_INSTANCE_UID_ANNOTATION_KEY = "pandora.dev/ds-instance-uid"
DS_INSTANCE_EPOCH_ANNOTATION_KEY = "pandora.dev/ds-instance-epoch"
DS_WRITER_EPOCH_ANNOTATION_KEY = "pandora.dev/ds-writer-epoch"
DS_TOKEN_KID_ANNOTATION_KEY = "pandora.dev/ds-token-kid"
DS_TOKEN_HASH_ANNOTATION_KEY = "pandora.dev/ds-token-sha256"

#: Model B bundle 的 9 个 annotation key,顺序与 Go 的 `dsCredentialAnnotationKeys`
#: 一致(JSON Patch 的 op 顺序照它生成)。
DS_CREDENTIAL_ANNOTATION_KEYS: tuple[str, ...] = (
    DS_TOKEN_ANNOTATION_KEY,
    DS_TOKEN_EXP_ANNOTATION_KEY,
    DS_TOKEN_GEN_ANNOTATION_KEY,
    DS_TOKEN_JTI_ANNOTATION_KEY,
    DS_INSTANCE_UID_ANNOTATION_KEY,
    DS_INSTANCE_EPOCH_ANNOTATION_KEY,
    DS_WRITER_EPOCH_ANNOTATION_KEY,
    DS_TOKEN_KID_ANNOTATION_KEY,
    DS_TOKEN_HASH_ANNOTATION_KEY,
)

_HTTP_CONFLICT = 409
_HTTP_NOT_FOUND = 404
_MAX_BODY_BYTES = 1 << 20  # Go: io.LimitReader(resp.Body, 1<<20)
_MAX_CAS = 3  # Go: const maxCAS = 3


class AgonesError(RuntimeError):
    """Agones / k8s REST 交互失败。对应 Go 侧那一族 `fmt.Errorf("agones: ...")`。"""


# ── GameServer / Pod / Fleet 响应 JSON(只声明用到的字段)────────────────────
#
# 解析刻意**严格**:字段类型不对就抛,而不是宽容地当空值。Go 的 json.Unmarshal 本来
# 就是这个行为,而这里的宽容会把「apiserver 回了个我们看不懂的东西」悄悄变成
# 「这台 GameServer 没有 annotation」,再变成「重签一张新令牌覆盖掉好的那张」。


def _as_str(raw: dict[str, Any], key: str) -> str:
    value = raw.get(key)
    if value is None:
        return ""
    if not isinstance(value, str):
        raise AgonesError(f"agones: field {key!r} must be a string, got {type(value).__name__}")
    return value


def _as_str_map(raw: dict[str, Any], key: str) -> dict[str, str] | None:
    """返回 `None` 表示该字段整体缺失 —— 对应 Go 的 nil map。

    ★ `None` 与 `{}` 必须区分:`_patch_credential_annotation` 用它决定是
      `add /metadata/annotations`(整体创建)还是逐 key `add`。对一个**不存在**的
      annotations 逐 key add,RFC-6902 会因父路径不存在而整片失败。
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
class GSPort:
    name: str = ""
    port: int = 0


@dataclasses.dataclass(slots=True)
class GSStatus:
    state: str = ""
    address: str = ""
    ports: list[GSPort] = dataclasses.field(default_factory=list)


@dataclasses.dataclass(slots=True)
class GSMetadata:
    name: str = ""
    labels: dict[str, str] = dataclasses.field(default_factory=dict)
    #: `None` = 对象上根本没有 annotations 字段(Go 的 nil map)。
    annotations: dict[str, str] | None = None
    #: 供令牌 annotation 重签做乐观并发 CAS:PATCH 携带它,apiserver 在 live 对象 rv
    #: 已变时回 409 Conflict,冲突方重读再判定,避免多副本交错覆盖。
    resource_version: str = ""
    uid: str = ""

    def ann(self, key: str) -> str:
        """读 annotation;缺 annotations 或缺 key 都返回 ""(Go 读 nil map 的语义)。"""
        if self.annotations is None:
            return ""
        return self.annotations.get(key, "")


@dataclasses.dataclass(slots=True)
class GameServer:
    metadata: GSMetadata = dataclasses.field(default_factory=GSMetadata)
    status: GSStatus = dataclasses.field(default_factory=GSStatus)


@dataclasses.dataclass(slots=True)
class OwnerReference:
    kind: str = ""
    uid: str = ""


@dataclasses.dataclass(slots=True)
class KubernetesPod:
    name: str = ""
    uid: str = ""
    owner_references: list[OwnerReference] = dataclasses.field(default_factory=list)


def _parse_game_server(raw: Any) -> GameServer:
    if not isinstance(raw, dict):
        raise AgonesError("agones: gameserver payload is not an object")
    md_raw = raw.get("metadata") or {}
    if not isinstance(md_raw, dict):
        raise AgonesError("agones: gameserver metadata is not an object")
    st_raw = raw.get("status") or {}
    if not isinstance(st_raw, dict):
        raise AgonesError("agones: gameserver status is not an object")

    ports: list[GSPort] = []
    ports_raw = st_raw.get("ports") or []
    if not isinstance(ports_raw, list):
        raise AgonesError("agones: gameserver status.ports is not an array")
    for p in ports_raw:
        if not isinstance(p, dict):
            raise AgonesError("agones: gameserver status.ports item is not an object")
        port = p.get("port", 0)
        if not isinstance(port, int) or isinstance(port, bool):
            raise AgonesError("agones: gameserver port must be an integer")
        ports.append(GSPort(name=_as_str(p, "name"), port=port))

    return GameServer(
        metadata=GSMetadata(
            name=_as_str(md_raw, "name"),
            labels=_as_str_map(md_raw, "labels") or {},
            annotations=_as_str_map(md_raw, "annotations"),
            resource_version=_as_str(md_raw, "resourceVersion"),
            uid=_as_str(md_raw, "uid"),
        ),
        status=GSStatus(
            state=_as_str(st_raw, "state"),
            address=_as_str(st_raw, "address"),
            ports=ports,
        ),
    )


def _parse_pod(raw: Any) -> KubernetesPod:
    if not isinstance(raw, dict):
        raise AgonesError("agones: pod payload is not an object")
    md_raw = raw.get("metadata") or {}
    if not isinstance(md_raw, dict):
        raise AgonesError("agones: pod metadata is not an object")
    owners: list[OwnerReference] = []
    owners_raw = md_raw.get("ownerReferences") or []
    if not isinstance(owners_raw, list):
        raise AgonesError("agones: pod metadata.ownerReferences is not an array")
    for o in owners_raw:
        if not isinstance(o, dict):
            raise AgonesError("agones: pod ownerReferences item is not an object")
        owners.append(OwnerReference(kind=_as_str(o, "kind"), uid=_as_str(o, "uid")))
    return KubernetesPod(
        name=_as_str(md_raw, "name"), uid=_as_str(md_raw, "uid"), owner_references=owners
    )


# ── 令牌 / 凭据依赖的注入接口 ────────────────────────────────────────────────


@dataclasses.dataclass(slots=True)
class HubCredentialClaims:
    """已经由 pkg/auth Verifier 完整验签后的 Model B JWT tuple。对应 Go 同名结构。

    `kid` 同时存在于签名 claim 与 JWT header;`token_sha256` 再绑定完整
    header.payload.signature。
    """

    pod: str = ""
    instance_uid: str = ""
    protocol_epoch: int = 0
    gen: int = 0
    jti: str = ""
    exp_ms: int = 0
    kid: str = ""
    writer_epoch: int = 0


@runtime_checkable
class HubAuthRepo(Protocol):
    """Model B 授权记录仓(Redis 是唯一权威)。对应 Go 的 `data.HubAuthRepo` 中
    fleet 层用到的那 4 个方法。

    实现在 `auth_repo.RedisHubAuthRepo`。

    ★ TTL 参数是**秒**(float),不是 `timedelta` —— 跟随 `auth_repo` / `ledger`
      整层的约定。Go 那边两边都是 `time.Duration` 所以不会错;Python 的
      `int(timedelta * 1000)` 直接 TypeError,而那一条路径只有真接上 Redis 仓才走得到,
      单看本文件的类型声明发现不了。公开的 `set_hub_authority(auth_ttl=timedelta)`
      保持与 Go 签名一致,在调用点统一转秒。
    """

    async def init_auth(
        self, pod: str, instance_uid: str, auth_ttl_sec: float
    ) -> hubv1.HubShardAuthStorageRecord:
        """确保授权记录存在并绑定当前 GameServer 实例;换实例 / 首见 → 复位 BOOTSTRAP。"""
        ...

    async def get_auth(
        self, pod: str
    ) -> tuple[hubv1.HubShardAuthStorageRecord | None, bool]:
        """读授权记录。返回 `(rec, found)`;IO 失败抛异常(Go 的第三个返回值)。"""
        ...

    async def stage_pending(
        self, pod: str, cred: hubv1.HubDSCredential, auth_ttl_sec: float
    ) -> hubv1.HubShardAuthStorageRecord:
        """WATCH/CAS 暂存 pending 凭据。"""
        ...

    async def mark_delivered(
        self,
        pod: str,
        expected: hubv1.HubDSCredential,
        resource_version: str,
        auth_ttl_sec: float,
    ) -> None:
        """以 expected tuple 再做一次 CAS,记录 annotation 投递已确认。"""
        ...


#: legacy 单阶段签发器:pod -> (token, exp_ms, gen);失败抛异常。
DSTokenIssuer = Callable[[str], Awaitable[tuple[str, int, int]]]
#: legacy 现存令牌验签器:(token, pod) -> None;不通过抛异常。
DSTokenVerifier = Callable[[str, str], Awaitable[None]]
#: Model B 凭据签发器:(pod, instance_uid, epoch) -> (token, cred);失败抛异常。
HubCredIssuer = Callable[[str, str, int], Awaitable[tuple[str, hubv1.HubDSCredential]]]
#: Model B 凭据验签器:token -> claims;不通过抛异常。
HubCredVerifier = Callable[[str], Awaitable[HubCredentialClaims]]


@dataclasses.dataclass(slots=True)
class KubeResponse:
    """一次 REST 调用的结果。对应 Go 的 `(body, statusCode, transportErr)` 三返回值。

    ★ 传输错误**不抛异常**,而是塞进 `error` 字段。这不是偷懒:
      `_patch_credential_annotation` 必须在 PATCH **超时 / 连接失败**之后仍然去跑
      一次确认 GET —— 因为「请求超时」完全不能证明「服务端没应用」。写成抛异常,
      调用点就会在 `await` 处直接跳走,那段 fail-closed 的确认逻辑整块失效,
      于是一次网络抖动就被当成「投递失败」,下轮重签一张新令牌把已经送达的旧令牌
      覆盖掉,DS 手里的令牌永远追不上 Redis。
    """

    body: bytes = b""
    status: int = 0
    error: Exception | None = None


def escape_json_pointer(s: str) -> str:
    """RFC 6901 转义。对应 Go 的 `escapeJSONPointer`。

    ★ 顺序不能反:必须先换 `~` 再换 `/`。反过来的话 `/` 先变成 `~1`,
      随后那个新生成的 `~` 又被当成原文的 `~` 换成 `~01`,路径直接错。
      annotation key 里带 `/`(`pandora.dev/ds-token`)是常态,这条每次都会走到。
    """
    return s.replace("~", "~0").replace("/", "~1")


def truncate_body(b: bytes, n: int) -> str:
    """截断 body 给错误信息用,避免日志过长。对应 Go 的 `truncateBody`。"""
    if len(b) <= n:
        return b.decode("utf-8", errors="replace")
    return b[:n].decode("utf-8", errors="replace") + "..."


def _fnv1a32(data: bytes) -> int:
    """FNV-1a 32bit。对应 Go 的 `hash/fnv.New32a()`。

    手写而非用 `hash()`:Python 的内建 hash 每进程加盐,同一个 pod 名在两次进程里
    会算出不同 shard_id,而 shard_id 会被写进 Redis 分片记录 —— 重启一次就全表漂移。
    """
    h = 0x811C9DC5
    for byte in data:
        h ^= byte
        h = (h * 0x01000193) & 0xFFFFFFFF
    return h


def shard_id_for(gs: GameServer) -> int:
    """取 GameServer 的稳定 shard_id。对应 Go 的 `shardIDFor`。

    优先读 `pandora.dev/shard-id` 标签,缺省 / 非法则按 pod 名 FNV-1a 哈希派生
    (仅作并列 tiebreak / 展示)。哈希结果为 0 时抬成 1 —— 0 在下游被当作"未设置"。
    """
    v = gs.metadata.labels.get(SHARD_ID_LABEL_KEY)
    if v is not None:
        try:
            n = int(v)
        except ValueError:
            n = -1
        if 0 < n <= 0xFFFFFFFF:
            return n
    shard_id = _fnv1a32(gs.metadata.name.encode("utf-8"))
    return shard_id or 1


def capacity_for(gs: GameServer, fallback: int) -> int:
    """取 GameServer 的容量。对应 Go 的 `capacityFor`。

    优先读 `pandora.dev/capacity` 标签,缺省 / 非法用 fallback。
    """
    v = gs.metadata.labels.get(CAPACITY_LABEL_KEY)
    if v is not None:
        try:
            n = int(v)
        except ValueError:
            n = 0
        if 0 < n <= 0x7FFFFFFF:
            return n
    return fallback


def exp_from_annotations(gs: GameServer) -> int:
    """读回令牌 exp(`pandora.dev/ds-token-exp-ms`);缺失 / 非法为 0。"""
    s = gs.metadata.ann(DS_TOKEN_EXP_ANNOTATION_KEY)
    if s == "":
        return 0
    try:
        return int(s)
    except ValueError:
        return 0


def gen_from_annotations(gs: GameServer) -> int:
    """读回当前令牌代际(`pandora.dev/ds-token-gen`);缺失 / 非法为 0。

    保留旧令牌(未重签)时用它把 annotation 镜像的代际透传给拓扑对账,避免代际被误清 0。
    """
    s = gs.metadata.ann(DS_TOKEN_GEN_ANNOTATION_KEY)
    if s == "":
        return 0
    try:
        n = int(s)
    except ValueError:
        return 0
    return n if n >= 0 else 0


def hub_credential_annotations(token: str, expected: hubv1.HubDSCredential) -> dict[str, str]:
    """拼 Model B 的 9 项 annotation bundle。对应 Go 的 `hubCredentialAnnotations`。"""
    return {
        DS_TOKEN_ANNOTATION_KEY: token,
        DS_TOKEN_EXP_ANNOTATION_KEY: str(expected.exp_ms),
        DS_TOKEN_GEN_ANNOTATION_KEY: str(expected.gen),
        DS_TOKEN_JTI_ANNOTATION_KEY: expected.jti,
        DS_INSTANCE_UID_ANNOTATION_KEY: expected.instance_uid,
        DS_INSTANCE_EPOCH_ANNOTATION_KEY: str(expected.protocol_epoch),
        DS_WRITER_EPOCH_ANNOTATION_KEY: str(expected.writer_epoch),
        DS_TOKEN_KID_ANNOTATION_KEY: expected.kid,
        DS_TOKEN_HASH_ANNOTATION_KEY: expected.token_sha256,
    }


def credential_fields_equal(
    a: hubv1.HubDSCredential | None, b: hubv1.HubDSCredential | None
) -> bool:
    """两份凭据的八元组是否逐字段相等。对应 Go 的 `credentialFieldsEqual`。

    刻意**不用** `a == b`(proto 的整体相等):存储记录以后可能加字段,整体相等会把
    「新增了一个无关字段」判成「凭据变了」,于是每轮对账都重签一次令牌。
    """
    if a is None or b is None:
        return False
    return (
        a.gen == b.gen
        and a.jti == b.jti
        and a.exp_ms == b.exp_ms
        and a.kid == b.kid
        and a.instance_uid == b.instance_uid
        and a.protocol_epoch == b.protocol_epoch
        and a.token_sha256 == b.token_sha256
        and a.writer_epoch == b.writer_epoch
    )


def _opt_cred(
    rec: hubv1.HubShardAuthStorageRecord, field: str
) -> hubv1.HubDSCredential | None:
    """proto 的 message 字段 presence → `None`,对应 Go 的 `rec.Active != nil`。

    不能用 `if rec.active:`:proto3 的 message 字段访问总会返回一个默认实例,
    而全零实例的布尔值是 False —— 「有一份全零凭据」和「没有凭据」会被混成一件事。
    """
    return getattr(rec, field) if rec.HasField(field) else None


class AgonesHubFleetProvider:
    """经 k8s apiserver REST 查 Agones GameServer 列表发现 Hub 分片拓扑。

    对应 Go 的 `AgonesHubFleetProvider`。同时实现 `HubFleetProvider`、
    `HubFleetPhysicalObserver` 与 `HubFleetScaler` 三个协议。

    构造失败场景(Go 返 error,这里抛 `ValueError`,main 据此 fatal 或回退):
      - `fleet_name` 空(无法选择 GameServer)
      - canary 策略非法 / `canary_percent > 0` 但 `canary_fleet_name` 空
      - 双轨 Fleet 同时开 `hub.autoscale_enabled`(scaler 只治理 stable Fleet,
        双轨时继续启用会把两轨总负载错误作用到单轨)
      - CA 文件配置了却解析失败
    """

    def __init__(
        self,
        cfg: hconf.Config,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        ag = cfg.agones
        if ag.fleet_name == "":
            raise ValueError("agones: fleet_name required when enabled")
        try:
            releasetrack.new(ag.canary_percent, ag.canary_seed)
        except ValueError as exc:
            raise ValueError(f"agones: invalid canary policy: {exc}") from exc
        if ag.canary_percent > 0 and ag.canary_fleet_name.strip() == "":
            raise ValueError("agones: canary_fleet_name required when canary_percent > 0")
        if ag.canary_fleet_name.strip() != "" and cfg.hub.autoscale_enabled:
            raise ValueError(
                "agones: hub autoscale is not supported with split stable/canary fleets"
            )

        timeout = ag.list_timeout_td().total_seconds()
        if timeout <= 0:
            timeout = hconf.DEFAULT_AGONES_LIST_TIMEOUT.total_seconds()
        capacity = cfg.hub.default_capacity
        if capacity <= 0:
            capacity = 500

        self._api_server = ag.api_server.rstrip("/")
        self._namespace = ag.namespace
        self._fleet_name = ag.fleet_name
        self._canary_fleet_name = ag.canary_fleet_name.strip()
        self._advertise_host = ag.advertise_host.strip()
        self._token_path = ag.token_path  # "" 或 "-" → 不带 Authorization
        self._list_timeout = timeout
        self._capacity = capacity

        if transport is not None:
            # 测试用假 apiserver:此时不建 TLS 上下文(MockTransport 不走网络)。
            self._client = httpx.AsyncClient(transport=transport, timeout=timeout)
        else:
            self._client = httpx.AsyncClient(
                verify=_build_ssl_context(ag), timeout=timeout
            )

        # legacy 代际门单阶段签发
        self._ds_token_issuer: DSTokenIssuer | None = None
        self._ds_token_verify: DSTokenVerifier | None = None
        self._ds_token_renew_before = _dt.timedelta(0)
        self._ds_token_required = False

        # Model B「Redis 唯一授权权威」两阶段投递
        self._auth_repo: HubAuthRepo | None = None
        self._hub_cred_issuer: HubCredIssuer | None = None
        self._hub_cred_verifier: HubCredVerifier | None = None
        self._auth_ttl = _dt.timedelta(0)
        #: 区分「完全没启用 Model B」与「Model B 装配缺依赖」。后者必须 fail-closed,
        #: 绝不能静默回退 legacy / no-token 路径。
        self._hub_authority_configured = False

    # ── 依赖注入 ──────────────────────────────────────────────────────────

    def set_ds_token_issuer(
        self, issuer: DSTokenIssuer | None, renew_before: _dt.timedelta, required: bool
    ) -> None:
        """注入 legacy DS 回调令牌签发器。对应 Go 的 `SetDSTokenIssuer`。

        `renew_before`:剩余寿命小于此值时重签续期(建议 TTL/3)。
        `required=True`(guard=enforce)时签发 / patch 失败的 Hub DS 会在
        `list_shards` 被标记 `token_ready=False`(fail-closed)。
        """
        self._ds_token_issuer = issuer
        self._ds_token_required = required
        if renew_before > _dt.timedelta(0):
            self._ds_token_renew_before = renew_before

    def set_ds_token_verifier(self, verifier: DSTokenVerifier | None) -> None:
        """注入现存令牌验签器(可选,须在首次 `list_shards` 前调用)。

        对应 Go 的 `SetDSTokenVerifier`。供 `_ensure_ds_token` 在"exp 未近"之外再
        实测 annotation 令牌确实验签通过(挡空 / 损坏 / 旧密钥令牌)。
        """
        self._ds_token_verify = verifier

    def set_hub_authority(
        self,
        auth_repo: HubAuthRepo | None,
        issuer: HubCredIssuer | None,
        verifier: HubCredVerifier | None,
        renew_before: _dt.timedelta,
        auth_ttl: _dt.timedelta,
    ) -> None:
        """启用 Model B 两阶段令牌投递(仅 `authority_mode=redis`)。

        对应 Go 的 `SetHubAuthority`。调用本方法后任一依赖缺失都 fail-closed。
        """
        self._hub_authority_configured = True
        self._auth_repo = auth_repo
        self._hub_cred_issuer = issuer
        self._hub_cred_verifier = verifier
        self._ds_token_required = True
        if renew_before > _dt.timedelta(0):
            self._ds_token_renew_before = renew_before
        if auth_ttl > _dt.timedelta(0):
            self._auth_ttl = auth_ttl

    def _model_b_active(self) -> bool:
        """本 provider 是否处于 Model B 两阶段投递模式。对应 Go 的 `modelBActive`。"""
        return (
            self._hub_authority_configured
            and self._auth_repo is not None
            and self._hub_cred_issuer is not None
            and self._hub_cred_verifier is not None
            and self._auth_ttl > _dt.timedelta(0)
        )

    async def aclose(self) -> None:
        """关闭底层 HTTP 连接池。"""
        await self._client.aclose()

    # ── HubFleetProvider ──────────────────────────────────────────────────

    async def list_shards(self, region: str) -> list[ShardCandidate]:
        """分别发现 stable / canary。对应 Go 的 `ListShards`。

        ★ 配置只决定要查哪个 Fleet;最终 `release_track` 必须由 GameServer
          label + annotation 一致证明,不能把 cohort 意图当成实际命中轨。
        """
        out = await self._list_track_shards(region, self._fleet_name, releasetrack.STABLE)
        if self._canary_fleet_name != "":
            out = out + await self._list_track_shards(
                region, self._canary_fleet_name, releasetrack.CANARY
            )
        return out

    async def _list_track_shards(
        self, region: str, fleet_name: str, release_track: str
    ) -> list[ShardCandidate]:
        if fleet_name == "" or not releasetrack.valid(release_track):
            raise AgonesError(
                f"agones: invalid fleet/release track pair "
                f"fleet={fleet_name!r} track={release_track!r}"
            )
        selector = (
            f"{FLEET_LABEL_KEY}={fleet_name},{RELEASE_TRACK_METADATA_KEY}={release_track}"
        )
        if region != "":
            selector += f",{REGION_LABEL_KEY}={region}"
        query = urllib.parse.urlencode({"labelSelector": selector})
        list_url = (
            f"{self._api_server}/apis/agones.dev/v1/namespaces/{self._namespace}"
            f"/gameservers?{query}"
        )

        resp = await self._do("GET", list_url)
        where = f"fleet={fleet_name} track={release_track} region={region}"
        if resp.error is not None:
            raise AgonesError(f"agones: list gameservers {where}: {resp.error}") from resp.error
        if not _is_2xx(resp.status):
            raise AgonesError(
                f"agones: list gameservers {where} http {resp.status}: "
                f"{truncate_body(resp.body, 256)}"
            )
        try:
            payload = json.loads(resp.body)
        except ValueError as exc:
            raise AgonesError(f"agones: decode gameserver list: {exc}") from exc
        items = payload.get("items") if isinstance(payload, dict) else None
        if items is None:
            items = []
        if not isinstance(items, list):
            raise AgonesError("agones: decode gameserver list: items is not an array")

        out: list[ShardCandidate] = []
        for raw in items:
            gs = _parse_game_server(raw)
            # ★ 轨道判定是**严格相等**,没有"缺了就当 stable"这条退路。
            #   GameServer 是编排层的实时对象,不存在「滚动升级前的历史记录」这回事;
            #   把一台没打轨道标签的 canary GameServer 默认成 stable,等于把灰度实例
            #   混进正式池(§9.21 明令禁止)。持久化记录的空值兼容规则在
            #   `fleet.sticky_release_track`,两处方向相反是分工不是笔误。
            if (
                gs.metadata.labels.get(FLEET_LABEL_KEY, "") != fleet_name
                or gs.metadata.labels.get(RELEASE_TRACK_METADATA_KEY, "") != release_track
                or gs.metadata.ann(RELEASE_TRACK_METADATA_KEY) != release_track
            ):
                plog.get().warning(
                    "hub_gameserver_release_track_metadata_invalid",
                    pod=gs.metadata.name,
                    fleet=fleet_name,
                    release_track=release_track,
                )
                continue
            if gs.status.state not in HUB_READY_STATES:
                continue
            if gs.status.address == "" or not gs.status.ports:
                continue  # 尚未就绪(无 address/port),跳过

            # DS 回调服务令牌:annotation 缺失 / 即将过期 / 验签不过 → 重签 + patch。
            # enforce(ds_token_required)下签发 / patch 失败 → token_ready=False:
            # 该分片仍在拓扑里返回(供对账区分"Fleet 里没有" vs "Fleet 里有但令牌不可用"),
            # 但不会被当作可用镜像分配出去 —— 原来直接 skip 会让全 region 令牌失败时
            # 对账误判 Fleet 空而保留旧 ready 镜像。
            # off / permissive 下令牌失败不影响可用性,token_ready 恒 True。
            token_ready = True
            token_exp_ms = 0
            token_gen = 0
            try:
                token_exp_ms, token_gen = await self._ensure_ds_token_or_credential(gs)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 — 与 Go 的 `terr != nil` 同宽
                token_exp_ms, token_gen = 0, 0
                if self._ds_token_required:
                    token_ready = False
                    plog.get().warning(
                        "hub_ds_token_required_unusable",
                        pod=gs.metadata.name,
                        err=repr(exc),
                        mode="enforce",
                        hint="ds_auth.mode=enforce 下签发/patch 失败:该 Hub 令牌不可用,不进可用镜像",
                    )
            host = self._advertise_host or gs.status.address
            out.append(
                ShardCandidate(
                    pod_name=gs.metadata.name,
                    addr=f"{host}:{gs.status.ports[0].port}",
                    region=region,
                    shard_id=shard_id_for(gs),
                    capacity=capacity_for(gs, self._capacity),
                    release_track=release_track,
                    token_ready=token_ready,
                    token_exp_ms=token_exp_ms,
                    token_gen=token_gen,
                    # instance_uid / protocol_epoch 刻意留空:线上实例身份由 Model B
                    # 授权记录 promote 后投影,不能由拓扑发现抢先写。
                )
            )
        return out

    # ── HubFleetPhysicalObserver ──────────────────────────────────────────

    async def observe_shard_instance(self, pod: str) -> HubInstanceObservation:
        """读**未经过滤**的 GameServer 与它的 Pod。对应 Go 的 `ObserveShardInstance`。

        ★ 刻意不看 readiness / health / address / release-track / 回调令牌:
          那些是**路由事实**,不是**物理存活事实**。用它们过滤会让一台
          Unhealthy 但进程还活着、还握着玩家的 Hub 被判成"已拆机"。
        """
        if pod.strip() == "":
            raise AgonesError("agones: observe hub shard requires pod")
        obs = HubInstanceObservation()
        gs_url = (
            f"{self._api_server}/apis/agones.dev/v1/namespaces/{self._namespace}"
            f"/gameservers/{urllib.parse.quote(pod, safe='')}"
        )
        gs_resp = await self._do("GET", gs_url)
        if gs_resp.error is not None:
            raise AgonesError(
                f"agones: observe gameserver {pod}: {gs_resp.error}"
            ) from gs_resp.error
        if gs_resp.status == _HTTP_NOT_FOUND:
            pass  # GameServer 对象已不存在 —— 这本身就是证据的一半
        elif _is_2xx(gs_resp.status):
            gs = _parse_game_server(_load_json(gs_resp.body, f"observed gameserver {pod}"))
            if gs.metadata.name != pod or gs.metadata.uid == "":
                raise AgonesError(f"agones: observed gameserver {pod} missing exact identity")
            obs.game_server_found = True
            obs.game_server_uid = gs.metadata.uid
        else:
            raise AgonesError(
                f"agones: observe gameserver {pod} http {gs_resp.status}: "
                f"{truncate_body(gs_resp.body, 256)}"
            )

        pod_url = (
            f"{self._api_server}/api/v1/namespaces/{self._namespace}"
            f"/pods/{urllib.parse.quote(pod, safe='')}"
        )
        pod_resp = await self._do("GET", pod_url)
        if pod_resp.error is not None:
            raise AgonesError(f"agones: observe pod {pod}: {pod_resp.error}") from pod_resp.error
        if pod_resp.status == _HTTP_NOT_FOUND:
            return obs
        if _is_2xx(pod_resp.status):
            p = _parse_pod(_load_json(pod_resp.body, f"observed pod {pod}"))
            if p.name != pod or p.uid == "":
                raise AgonesError(f"agones: observed pod {pod} missing exact identity")
            obs.pod_found = True
            for owner in p.owner_references:
                if owner.kind == "GameServer" and owner.uid != "":
                    obs.pod_owner_game_server_uid = owner.uid
                    break
            return obs
        raise AgonesError(
            f"agones: observe pod {pod} http {pod_resp.status}: "
            f"{truncate_body(pod_resp.body, 256)}"
        )

    # ── 令牌供给 ──────────────────────────────────────────────────────────

    async def _ensure_ds_token_or_credential(self, gs: GameServer) -> tuple[int, int]:
        """令牌供给分发器。对应 Go 的 `ensureDSTokenOrCredential`。

        Model B(authority_mode=redis)走两阶段 pending 凭据投递;否则走 legacy 代际门
        单阶段签发。返回 `(当前生效令牌 exp_ms, gen)`,供 `list_shards` 判定
        `token_ready` / 写镜像。
        """
        if self._hub_authority_configured:
            if not self._model_b_active():
                # ★ 这里绝不能回退 legacy:装配缺依赖时静默降级,等于把「Redis 是唯一
                #   授权权威」这条设计在一次配置错误里悄悄关掉,而所有日志都正常。
                raise AgonesError("hub credential authority dependencies incomplete")
            return await self._ensure_hub_credential(gs)
        return await self._ensure_ds_token(gs)

    async def _ensure_hub_credential(self, gs: GameServer) -> tuple[int, int]:
        """Model B 两阶段令牌投递。对应 Go 的 `ensureHubCredential`。

        1. `init_auth`:确保授权记录存在并绑定当前 GameServer 实例(gs.uid);
           换实例 / 首见 → 复位 BOOTSTRAP。
        2. 若 annotation 现存令牌严格匹配 Redis active / pending 完整 tuple → 复用,
           不重签(收敛)。
        3. 否则领单调 gen + 生成 jti + 签发 pending 凭据,`stage_pending` 暂存。
        4. 用 JSON Patch 同时 test GameServer uid + resourceVersion 后投递 annotation bundle。
        5. 无论 PATCH 返回 2xx / 409 / 超时 / 坏 body,都 GET 严格确认对象终态 +
           Redis 当前 pending,再用 expected tuple CAS `mark_delivered`。
           **没有任何本地 gen / exp / rv fallback。**

        annotation 只负责把 token 送到 DS 手上;是否「授权生效」完全由 Redis 授权记录
        决定 —— DS 拿 pending token 发第一个合法心跳时才被原子激活。
        """
        assert self._auth_repo is not None  # _model_b_active 已保证
        assert self._hub_cred_issuer is not None
        pod = gs.metadata.name
        uid = gs.metadata.uid
        if uid == "":
            # GameServer 尚未被 apiserver 赋 uid(极早期);本轮跳过,下轮对账再投递。
            raise AgonesError(f"hub_credential: gameserver {pod} has empty uid")

        # 1. 绑定实例身份(换 DS 实例 → 复位 epoch++、清 active/pending)。
        try:
            rec = await self._auth_repo.init_auth(pod, uid, self._auth_ttl.total_seconds())
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            plog.get().warning(
                "hub_credential_init_failed", pod=pod, uid=uid, err=repr(exc)
            )
            raise
        if rec.phase not in (
            hubv1.HUB_AUTH_PHASE_BOOTSTRAP,
            hubv1.HUB_AUTH_PHASE_ACTIVE,
            hubv1.HUB_AUTH_PHASE_ROTATING,
        ):
            raise AgonesError(
                "hub credential auth phase does not allow delivery: "
                # ★ 走到这里就是因为 phase **不在**已知白名单里——包括“新副本写了一个
                # 旧副本不认识的 phase”。裸 `.Name()` 会在构造这条错误时抛 ValueError,
                # 把一次干净的凭据下发拒绝变成不可读的内部错误。
                f"{enum_name(hubv1.HubAuthPhase, rec.phase)}"
            )

        # 2a. 当前 annotation 严格等于 Redis active:已激活凭据仍可复用。
        active = _opt_cred(rec, "active")
        if active is not None and await self._credential_bundle_matches(gs, active, True) is None:
            return active.exp_ms, active.gen
        # 2b. 当前 annotation 严格等于 Redis pending:必须再 GET 当前对象终态,并以
        #     expected tuple CAS 记 delivered。LIST 快照 / annotation gen 数字本身
        #     没有推进授权的能力。
        pending = _opt_cred(rec, "pending")
        if pending is not None and await self._credential_bundle_matches(gs, pending, True) is None:
            rv = await self._confirm_credential_delivery(pod, pending)
            await self._auth_repo.mark_delivered(
                pod, pending, rv, self._auth_ttl.total_seconds()
            )
            return pending.exp_ms, pending.gen

        # 3. 签发新 pending 凭据并暂存(gen 单调 + jti 唯一,绑 uid+epoch)。
        try:
            token, cred = await self._hub_cred_issuer(pod, uid, rec.protocol_epoch)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            plog.get().warning("hub_credential_sign_failed", pod=pod, err=repr(exc))
            raise
        # 签发器的 token 与返回 credential 必须在写 Redis 前就自洽。否则拒绝 stage,
        # 防「凭据记录是一套、JWT 实际 claims 是另一套」的永久分裂。
        reason = await self._verify_token_against_credential(token, pod, cred, False)
        if reason is not None:
            plog.get().warning("hub_credential_issuer_mismatch", pod=pod, err=reason)
            raise AgonesError(reason)
        try:
            await self._auth_repo.stage_pending(pod, cred, self._auth_ttl.total_seconds())
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            plog.get().warning(
                "hub_credential_stage_failed",
                pod=pod,
                gen=cred.gen,
                err=repr(exc),
                hint="StagePending CAS 失败(uid/epoch/gen 竞态):本轮跳过,下轮对账重试",
            )
            raise

        # 4-5. uid+rv 条件投递 + GET 严格确认最终对象与 Redis 当前 pending。
        rv = await self._patch_credential_annotation(gs, token, cred)
        # mark_delivered 不是 best-effort:它必须以同一 expected tuple 再做一次 Redis
        # CAS。PATCH 后若 pending 已被更高代际替换,旧响应只会失败,绝不污染新 pending。
        try:
            await self._auth_repo.mark_delivered(pod, cred, rv, self._auth_ttl.total_seconds())
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            plog.get().warning("hub_credential_mark_delivered_failed", pod=pod, err=repr(exc))
            raise
        plog.get().info(
            "hub_credential_staged",
            pod=pod,
            uid=uid,
            epoch=rec.protocol_epoch,
            gen=cred.gen,
            exp_ms=cred.exp_ms,
        )
        return cred.exp_ms, cred.gen

    async def _patch_credential_annotation(
        self, gs: GameServer, token: str, expected: hubv1.HubDSCredential
    ) -> str:
        """严格投递 Model B annotation bundle。对应 Go 的 `patchCredentialAnnotation`。

        PATCH **只发一次**,不靠重试猜结果;无论 transport error、409、非 2xx、
        2xx 空 / 坏 / 缺字段或正常 2xx,都随后 GET 当前 GameServer 并验证
        JWT claims + annotation mirror + Redis 当前 pending 完全一致。

        ★ 两条 `test` op 就是本文件模块注释里讲的 **uid + resourceVersion 双 CAS**。
        ★ 判定成功的唯一标准是 **rv 前进了**。2xx 不算 —— apiserver 对一个内容完全
          相同的 PATCH 也回 2xx 但不推进 rv,那说明我们的写根本没落地(被别人抢先了)。
        """
        md = gs.metadata
        if md.name == "" or md.uid == "" or md.resource_version == "":
            raise AgonesError("hub credential patch requires gameserver name/uid/resourceVersion")
        if md.uid != expected.instance_uid:
            raise AgonesError("hub credential patch instance uid mismatch")
        reason = await self._verify_token_against_credential(token, md.name, expected, False)
        if reason is not None:
            raise AgonesError(reason)

        annotations = hub_credential_annotations(token, expected)
        ops: list[dict[str, Any]] = [
            {"op": "test", "path": "/metadata/uid", "value": md.uid},
            {"op": "test", "path": "/metadata/resourceVersion", "value": md.resource_version},
        ]
        if md.annotations is None:
            # annotations 整体缺失时一次创建;RV test 保证不会覆盖并发新增的 annotations。
            ops.append({"op": "add", "path": "/metadata/annotations", "value": annotations})
        else:
            for key in DS_CREDENTIAL_ANNOTATION_KEYS:
                ops.append(
                    {
                        "op": "add",
                        "path": "/metadata/annotations/" + escape_json_pointer(key),
                        "value": annotations[key],
                    }
                )
        patch = json.dumps(ops).encode("utf-8")
        gs_url = (
            f"{self._api_server}/apis/agones.dev/v1/namespaces/{self._namespace}"
            f"/gameservers/{md.name}"
        )
        resp = await self._do("PATCH", gs_url, patch, "application/json-patch+json")

        # PATCH 的任何返回都不是授权事实。即使传输层报错,也给安全确认一次独立、有界的
        # GET 机会(内部仍受 list_timeout 限制),避免「timeout 但实际已应用」被误判。
        confirm_err: Exception | None = None
        rv = ""
        try:
            rv = await self._confirm_credential_delivery(md.name, expected)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 — 与 Go 的 confirmErr 同宽
            confirm_err = exc
        if confirm_err is None and rv != md.resource_version:
            return rv
        if confirm_err is None:
            confirm_err = AgonesError("gameserver resourceVersion did not advance")
        if resp.error is not None:
            raise AgonesError(
                f"hub credential patch outcome unconfirmed after transport error: "
                f"{resp.error}; get confirm: {confirm_err}"
            ) from confirm_err
        raise AgonesError(
            f"hub credential patch outcome unconfirmed (http {resp.status}): {confirm_err}"
        ) from confirm_err

    async def _confirm_credential_delivery(
        self, pod: str, expected: hubv1.HubDSCredential
    ) -> str:
        """GET 当前 GameServer,严格确认对象 uid/RV、annotation bundle、JWT claims 与
        Redis **当前 pending** 都等于 expected。任何一项缺失 / 损坏 / 漂移都失败关闭。

        对应 Go 的 `confirmCredentialDelivery`。
        """
        assert self._auth_repo is not None
        current = await self._get_game_server(pod)
        if current.metadata.name != pod or current.metadata.resource_version == "":
            raise AgonesError("hub credential confirm missing gameserver identity/resourceVersion")
        reason = await self._credential_bundle_matches(current, expected, False)
        if reason is not None:
            raise AgonesError(f"hub credential confirm bundle mismatch: {reason}")
        try:
            rec, found = await self._auth_repo.get_auth(pod)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise AgonesError(f"hub credential confirm redis auth: {exc}") from exc
        if (
            not found
            or rec is None
            or rec.instance_uid != expected.instance_uid
            or rec.protocol_epoch != expected.protocol_epoch
            or not credential_fields_equal(_opt_cred(rec, "pending"), expected)
        ):
            raise AgonesError("hub credential confirm pending changed")
        if rec.phase in (
            hubv1.HUB_AUTH_PHASE_QUARANTINED,
            hubv1.HUB_AUTH_PHASE_TERMINATING,
        ):
            raise AgonesError("hub credential confirm auth phase locked")
        return current.metadata.resource_version

    async def _credential_bundle_matches(
        self, gs: GameServer, expected: hubv1.HubDSCredential, require_fresh: bool
    ) -> str | None:
        """annotation 是否为 expected 的**投递镜像**。匹配返回 `None`,否则返回原因。

        对应 Go 的 `credentialBundleMatches`(Go 返 error,这里返回原因串 —— 匹配失败
        在本文件是常规控制流,用异常表达会让"该不该重签"的判断混进 except 块)。

        ★ 绝不让 annotation 数字自己选择 / 推进 Redis 凭据:token 必须验签,且 claims、
          hash、外置 gen / exp 均**精确等于** expected。
        """
        md = gs.metadata
        if md.name == "" or md.uid == "" or md.resource_version == "":
            return "gameserver or expected credential incomplete"
        if md.uid != expected.instance_uid:
            return "gameserver uid mismatch"
        token = md.ann(DS_TOKEN_ANNOTATION_KEY)
        if token == "":
            return "token annotation missing"
        if _parse_uint(md.ann(DS_TOKEN_GEN_ANNOTATION_KEY)) != expected.gen or expected.gen == 0:
            return "token gen annotation mismatch"
        if _parse_uint(md.ann(DS_TOKEN_EXP_ANNOTATION_KEY)) != expected.exp_ms or expected.exp_ms == 0:
            return "token exp annotation mismatch"
        if md.ann(DS_TOKEN_JTI_ANNOTATION_KEY) == "" or md.ann(
            DS_TOKEN_JTI_ANNOTATION_KEY
        ) != expected.jti:
            return "token jti annotation mismatch"
        if md.ann(DS_INSTANCE_UID_ANNOTATION_KEY) == "" or md.ann(
            DS_INSTANCE_UID_ANNOTATION_KEY
        ) != expected.instance_uid:
            return "instance uid annotation mismatch"
        epoch = _parse_uint(md.ann(DS_INSTANCE_EPOCH_ANNOTATION_KEY))
        if epoch == 0 or epoch != expected.protocol_epoch:
            return "instance epoch annotation mismatch"
        writer = _parse_uint(md.ann(DS_WRITER_EPOCH_ANNOTATION_KEY))
        if writer == 0 or writer != expected.writer_epoch:
            return "writer epoch annotation mismatch"
        if md.ann(DS_TOKEN_KID_ANNOTATION_KEY) == "" or md.ann(
            DS_TOKEN_KID_ANNOTATION_KEY
        ) != expected.kid:
            return "token kid annotation mismatch"
        if md.ann(DS_TOKEN_HASH_ANNOTATION_KEY) == "" or md.ann(
            DS_TOKEN_HASH_ANNOTATION_KEY
        ) != expected.token_sha256:
            return "token hash annotation mismatch"
        return await self._verify_token_against_credential(token, md.name, expected, require_fresh)

    async def _verify_token_against_credential(
        self,
        token: str,
        pod: str,
        expected: hubv1.HubDSCredential,
        require_fresh: bool,
    ) -> str | None:
        """验 token 与 expected 凭据是否同一份。对应 Go 的 `verifyTokenAgainstCredential`。

        匹配返回 `None`,否则返回原因串。
        """
        if (
            expected.gen == 0
            or expected.jti == ""
            or expected.exp_ms == 0
            or expected.kid == ""
            or expected.instance_uid == ""
            or expected.protocol_epoch == 0
            or expected.token_sha256 == ""
            or expected.writer_epoch == 0
        ):
            return "expected hub credential incomplete"
        now_ms = int(time.time() * 1000)
        if now_ms < 0 or expected.exp_ms <= now_ms:
            return "expected hub credential expired"
        if require_fresh and self._ds_token_renew_before > _dt.timedelta(0):
            remaining_ms = expected.exp_ms - now_ms
            if remaining_ms <= int(self._ds_token_renew_before.total_seconds() * 1000):
                return "expected hub credential within renew window"
        if hashlib.sha256(token.encode("utf-8")).hexdigest() != expected.token_sha256:
            return "hub credential token hash mismatch"
        assert self._hub_cred_verifier is not None
        try:
            claims = await self._hub_cred_verifier(token)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 — 与 Go 的 `err != nil` 同宽
            return f"hub credential jwt verify: {exc}"
        if (
            claims is None
            or claims.pod != pod
            or claims.instance_uid != expected.instance_uid
            or claims.protocol_epoch != expected.protocol_epoch
            or claims.gen != expected.gen
            or claims.jti != expected.jti
            or claims.exp_ms != expected.exp_ms
            or claims.kid != expected.kid
            or claims.writer_epoch != expected.writer_epoch
        ):
            return "hub credential jwt tuple mismatch"
        return None

    # ── legacy 单阶段代际门 ───────────────────────────────────────────────

    async def _ensure_ds_token(self, gs: GameServer) -> tuple[int, int]:
        """保证 ready 的 Hub DS GameServer 持有未过期的 DS 回调令牌 annotation。

        对应 Go 的 `ensureDSToken`。

        并发安全:重签 PATCH 携带 `metadata.resourceVersion` 做**乐观并发 CAS**。
        多副本交错 patch 同一 GameServer 时,基于旧 rv 的写被 apiserver 以 409
        Conflict 拒,冲突方**重读对象再判定** —— 对方可能已写好当前代际令牌则直接
        复用(不再 INCR),避免「后到低代际 PATCH 覆盖高代际」导致 K8s 最终 gen 与
        Redis CurrentTokenGen 分裂。
        """
        if self._ds_token_issuer is None:
            return 0, 0
        cur = gs
        gs_url = (
            f"{self._api_server}/apis/agones.dev/v1/namespaces/{self._namespace}"
            f"/gameservers/{gs.metadata.name}"
        )
        for attempt in range(_MAX_CAS):
            # 现存令牌仍可复用(exp 未近 + 验签过 + enforce 下有合法 gen)→ 不重签。
            still_valid = await self._token_still_valid(cur)
            if still_valid is not None:
                return still_valid

            # 需重签:领取新单调代际并签发。
            try:
                token, exp_ms, gen = await self._ds_token_issuer(cur.metadata.name)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                plog.get().warning(
                    "hub_ds_token_sign_failed", pod=cur.metadata.name, err=repr(exc)
                )
                raise
            try:
                patch = json.dumps(
                    {
                        "metadata": {
                            # 乐观并发前置:rv 不匹配 → 409
                            "resourceVersion": cur.metadata.resource_version,
                            "annotations": {
                                DS_TOKEN_ANNOTATION_KEY: token,
                                DS_TOKEN_EXP_ANNOTATION_KEY: str(exp_ms),
                                DS_TOKEN_GEN_ANNOTATION_KEY: str(gen),
                            },
                        }
                    }
                ).encode("utf-8")
            except (TypeError, ValueError) as exc:
                plog.get().warning(
                    "hub_ds_token_patch_marshal_failed", pod=cur.metadata.name, err=repr(exc)
                )
                raise AgonesError(f"hub_ds_token patch marshal failed: {exc}") from exc

            resp = await self._do("PATCH", gs_url, patch, "application/merge-patch+json")
            if resp.error is not None:
                plog.get().warning(
                    "hub_ds_token_patch_failed", pod=cur.metadata.name, err=repr(resp.error)
                )
                raise AgonesError(
                    f"hub_ds_token patch {cur.metadata.name}: {resp.error}"
                ) from resp.error
            if resp.status == _HTTP_CONFLICT:
                # CAS 冲突:另一副本已改该 GameServer。重读拿最新 rv/annotation 再重试
                # (下轮 _token_still_valid 可能直接命中对方写的当前代际令牌,天然收敛
                # 不重复发号)。
                try:
                    refreshed = await self._get_game_server(cur.metadata.name)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    plog.get().warning(
                        "hub_ds_token_conflict_reget_failed",
                        pod=cur.metadata.name,
                        err=repr(exc),
                    )
                    raise
                plog.get().debug(
                    "hub_ds_token_patch_conflict_retry",
                    pod=cur.metadata.name,
                    attempt=attempt + 1,
                )
                cur = refreshed
                continue
            if not _is_2xx(resp.status):
                plog.get().warning(
                    "hub_ds_token_patch_failed",
                    pod=cur.metadata.name,
                    http_status=resp.status,
                    body=truncate_body(resp.body, 256),
                    hint=(
                        "检查 RBAC 是否对 gameservers 资源授予 patch 动词"
                        "(deploy/k8s/agones/10-rbac-allocator.yaml)"
                    ),
                )
                raise AgonesError(
                    f"hub_ds_token patch {cur.metadata.name} http {resp.status}"
                )
            # read-after-write:以服务器返回的最终对象 annotation 为准,不盲信本地 gen。
            eff_exp, eff_gen = exp_ms, gen
            try:
                updated = _parse_game_server(json.loads(resp.body))
            except (ValueError, AgonesError):
                updated = None
            if updated is not None:
                g = gen_from_annotations(updated)
                if g != 0:
                    eff_gen = g
                e = exp_from_annotations(updated)
                if e != 0:
                    eff_exp = e
            plog.get().info(
                "hub_ds_token_issued", pod=cur.metadata.name, exp_ms=eff_exp, gen=eff_gen
            )
            return eff_exp, eff_gen

        plog.get().warning(
            "hub_ds_token_cas_exhausted",
            pod=gs.metadata.name,
            hint=(
                "resourceVersion CAS 连续冲突耗尽:本轮不把该 Hub 计入可用"
                "(enforce 下 fail-closed),下轮对账重试"
            ),
        )
        raise AgonesError(f"hub_ds_token CAS retries exhausted for {gs.metadata.name}")

    async def _token_still_valid(self, gs: GameServer) -> tuple[int, int] | None:
        """现存 annotation 令牌是否仍可复用(无需重签)。对应 Go 的 `tokenStillValid`。

        需同时满足:①有非空 token ②外置 exp 未近 ③(启用验签时)实测验签通过
        ④enforce 下有合法代际(gen != 0)。满足返回 `(exp_ms, gen)`;任一不满足返回
        `None` 触发重签 —— 挡空 / 损坏 / 旧密钥令牌及 legacy gen0。
        """
        token = gs.metadata.ann(DS_TOKEN_ANNOTATION_KEY)
        if token == "":
            return None
        exp_str = gs.metadata.ann(DS_TOKEN_EXP_ANNOTATION_KEY)
        if exp_str == "":
            return None
        try:
            exp_ms = int(exp_str)
        except ValueError:
            return None
        renew_ms = int(self._ds_token_renew_before.total_seconds() * 1000)
        if exp_ms - int(time.time() * 1000) <= renew_ms:
            return None
        ann_gen = gen_from_annotations(gs)
        if self._ds_token_required and ann_gen == 0:
            # enforce 下缺合法代际(缺失 / 非法 / legacy gen0):强制重签补齐,否则该分片
            # 以 gen=0 进对账 → 心跳侧 gen_required 一律判 stale → 永不可分配;更挡
            # 「legacy gen0 被当有效而关闭代际门」。
            plog.get().warning(
                "hub_ds_token_missing_gen_resign",
                pod=gs.metadata.name,
                hint="enforce 下 annotation 无合法 ds-token-gen,强制重签补齐单调代际",
            )
            return None
        if self._ds_token_verify is not None:
            try:
                await self._ds_token_verify(token, gs.metadata.name)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 — 验不过就重签,原因不影响决策
                plog.get().warning(
                    "hub_ds_token_verify_failed_resign",
                    pod=gs.metadata.name,
                    hint="annotation 令牌验签不过(空/损坏/密钥轮换),触发重签",
                )
                return None
        return exp_ms, ann_gen

    async def _get_game_server(self, name: str) -> GameServer:
        """重读单个 GameServer(CAS 冲突后拿最新 resourceVersion + annotation)。"""
        gs_url = (
            f"{self._api_server}/apis/agones.dev/v1/namespaces/{self._namespace}"
            f"/gameservers/{name}"
        )
        resp = await self._do("GET", gs_url)
        if resp.error is not None:
            raise AgonesError(f"agones: get gameserver {name}: {resp.error}") from resp.error
        if not _is_2xx(resp.status):
            raise AgonesError(
                f"agones: get gameserver {name} http {resp.status}: "
                f"{truncate_body(resp.body, 256)}"
            )
        return _parse_game_server(_load_json(resp.body, f"gameserver {name}"))

    # ── HubFleetScaler ────────────────────────────────────────────────────

    async def get_fleet_replicas(self) -> int:
        """读取 Fleet 当前 `spec.replicas`。对应 Go 的 `GetFleetReplicas`。"""
        fleet_url = (
            f"{self._api_server}/apis/agones.dev/v1/namespaces/{self._namespace}"
            f"/fleets/{self._fleet_name}"
        )
        resp = await self._do("GET", fleet_url)
        if resp.error is not None:
            raise AgonesError(
                f"agones: get fleet {self._fleet_name}: {resp.error}"
            ) from resp.error
        if not _is_2xx(resp.status):
            raise AgonesError(
                f"agones: get fleet {self._fleet_name} http {resp.status}: "
                f"{truncate_body(resp.body, 256)}"
            )
        payload = _load_json(resp.body, f"fleet {self._fleet_name}")
        if not isinstance(payload, dict):
            raise AgonesError(f"agones: decode fleet {self._fleet_name}: not an object")
        spec = payload.get("spec") or {}
        if not isinstance(spec, dict):
            raise AgonesError(f"agones: decode fleet {self._fleet_name}: spec is not an object")
        replicas = spec.get("replicas", 0)
        if not isinstance(replicas, int) or isinstance(replicas, bool):
            raise AgonesError(
                f"agones: decode fleet {self._fleet_name}: spec.replicas is not an integer"
            )
        return replicas

    async def set_fleet_replicas(self, replicas: int) -> None:
        """PATCH Fleet `spec.replicas`。对应 Go 的 `SetFleetReplicas`。"""
        if replicas < 0:
            replicas = 0
        fleet_url = (
            f"{self._api_server}/apis/agones.dev/v1/namespaces/{self._namespace}"
            f"/fleets/{self._fleet_name}"
        )
        patch_body = json.dumps({"spec": {"replicas": replicas}}).encode("utf-8")
        resp = await self._do("PATCH", fleet_url, patch_body, "application/merge-patch+json")
        if resp.error is not None:
            raise AgonesError(
                f"agones: patch fleet replicas={replicas}: {resp.error}"
            ) from resp.error
        if not _is_2xx(resp.status):
            raise AgonesError(
                f"agones: patch fleet replicas={replicas} http {resp.status}: "
                f"{truncate_body(resp.body, 256)}"
            )

    # ── 传输 ──────────────────────────────────────────────────────────────

    async def _do(
        self,
        method: str,
        req_url: str,
        body: bytes | None = None,
        content_type: str = "",
    ) -> KubeResponse:
        """发一次带鉴权的 REST 请求,返回 `(body, status, transport_error)`。

        对应 Go 的 `do`。传输错误不抛(理由见 `KubeResponse` 的 docstring)。
        """
        headers = {"Accept": "application/json"}
        if body and content_type != "":
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
                method, req_url, content=body, headers=headers, timeout=self._list_timeout
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 — 与 Go 的 transportErr 同宽
            return KubeResponse(b"", 0, exc)
        return KubeResponse(resp.content[:_MAX_BODY_BYTES], resp.status_code, None)


# ── 模块级小工具 ────────────────────────────────────────────────────────────


def _is_2xx(status: int) -> bool:
    return 200 <= status < 300


def _load_json(body: bytes, what: str) -> Any:
    try:
        return json.loads(body)
    except ValueError as exc:
        raise AgonesError(f"agones: decode {what}: {exc}") from exc


def _parse_uint(s: str) -> int:
    """把 annotation 里的十进制无符号串解析成 int;缺失 / 非法 / 负数一律 0。

    0 在所有调用点都会被判成 mismatch,所以「解析不出来」天然 fail-closed。
    """
    if s == "":
        return 0
    try:
        n = int(s)
    except ValueError:
        return 0
    return n if n >= 0 else 0


def _build_ssl_context(ag: hconf.AgonesConf) -> ssl.SSLContext | bool:
    """按 Go 的 `tls.Config` 语义建 SSL 上下文。

    ★ 提供了 CA 且能读到时,**只信这一个 CA**,不叠加系统根证书池 ——
      Go 那边是 `tlsCfg.RootCAs = pool`(整体替换)。用
      `ssl.create_default_context(cafile=...)` 会在系统根之上再加,等于放宽了信任面。
    ★ CA 文件不存在 → 跳过用系统根(in-cluster 默认路径在集群外不存在,Go 同)。
    ★ CA 文件存在但**解析失败** → 抛错。这不是可以降级的情况:它意味着运维以为
      配了专用 CA,实际却在用系统根验证一个私有 apiserver。
    """
    if ag.insecure_skip_tls_verify:
        return False
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.check_hostname = True
    ctx.verify_mode = ssl.CERT_REQUIRED
    pem: str | None = None
    if ag.ca_path != "":
        try:
            pem = pathlib.Path(ag.ca_path).read_text(encoding="ascii")
        except (OSError, UnicodeDecodeError):
            pem = None
    if pem is None:
        ctx.load_default_certs()
        return ctx
    try:
        ctx.load_verify_locations(cadata=pem)
    except (ssl.SSLError, ValueError) as exc:
        raise ValueError(f"agones: parse CA {ag.ca_path} failed") from exc
    return ctx
