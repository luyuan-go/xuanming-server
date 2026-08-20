"""hub_allocator **Fleet 拓扑发现层**回归测试。

覆盖三个模块:

    pandorapy/services/hub_allocator/fleet.py         契约 + Mock provider
    pandorapy/services/hub_allocator/local_fleet.py   本机 exec 的单分片 provider
    pandorapy/services/hub_allocator/agones_fleet.py  真 Agones/k8s provider

对应 Go 侧 `internal/biz/{fleet,local_fleet,agones_fleet}.go` 及其
`{local_fleet,agones_fleet,agones_credential_delivery}_test.go`。

## 本文件重点盯的四个危险点(设计文档点名)

  ① **resourceVersion 条件 PATCH**。Model B 的凭据投递用 JSON Patch 的两条 `test`
     op(uid + resourceVersion)做乐观并发闸。这道闸没了不会报错 —— 只会在多副本
     交错写同一个 GameServer 时,把别人刚写好的高代际凭据用自己的低代际盖掉,
     于是 K8s annotation 与 Redis 授权记录分叉,DS 拿着一份谁也不认的令牌。
     所以本文件的假 apiserver **真的执行 test op**(不成立就 409 且一个 op 都不应用),
     "闸失效"才会表现成"本不该落地的写落地了",而不是靠断言请求体形状间接推断。
  ② **空 release_track 回落 stable**。`sticky_release_track` 是**持久化记录**的旧值
     迁移规则;`agones_fleet` 的拓扑发现方向**相反**(严格相等,缺标签就跳过)。
     两处方向相反是分工不是笔误,所以两个方向各有一条测试钉住。
  ③ **Stable / Canary 分轨**(§9 不变量 21)。配置只决定查哪个 Fleet,最终轨道必须由
     GameServer 的 label + annotation **一致证明**。把 cohort 意图当成实际命中轨,
     等于把灰度实例混进正式池。
  ④ **local_fleet 与 agones_fleet 对同一份 `ShardCandidate` / `HubInstanceObservation`
     契约必须可互换**。两个 provider 被同一段 ensure_shards / 对账代码消费,任一侧
     私自扩大或缩小契约(比如 local 也去实现 Scaler、或 agones 抢先写 instance_uid),
     上层就会在"换个 mode"时行为漂移。

## 依赖策略

  * Redis:**优先真 Redis**(`PANDORA_TEST_REDIS_ADDR`,默认 `127.0.0.1:16379`),
    连不上回落 `fakeredis`,**从不 skip**。Model B 的授权权威就是 Redis,
    给它留"环境不好就不跑"的后门等于把最关键的几条断言变成可选项。
  * k8s apiserver:`httpx.MockTransport` 注入(`AgonesHubFleetProvider(cfg, transport=...)`
    是模块头明写的测试钩子),不 patch 任何内部状态。
  * 签发 / 验签:依赖注入的替身对象。真 RS256 只会给本文件加一层与被测行为无关的
    密码学噪音;被测契约是"token 与 credential tuple 必须自洽",不是"签名算得对"。

★ 每条用例 docstring 的 `★ 变异:` 一行记录"改坏哪一行会让本条变红" —— 都是真跑过的
  (改坏 → 红 → 改回 → 绿),不是照着代码猜的。
"""

from __future__ import annotations

import asyncio
import base64
import datetime as _dt
import hashlib
import json
import os
import time
import urllib.parse
from typing import Any, Callable

import httpx
import pytest
from pandora.hub.v1 import allocator_pb2 as hubpb

from pandorapy import errcode, releasetrack
from pandorapy.auth import DS_AUTH_WRITER_EPOCH_V2 as WRITER
from pandorapy.services.hub_allocator import agones_fleet as A
from pandorapy.services.hub_allocator import auth_repo as hauth
from pandorapy.services.hub_allocator import conf as hconf
from pandorapy.services.hub_allocator import fleet as F
from pandorapy.services.hub_allocator import local_fleet as LF

_API = "https://apiserver.test"
_NS = "pandora"
_REGION = "cn-north"
_AUTH_TTL = _dt.timedelta(minutes=30)
_RENEW_BEFORE = _dt.timedelta(minutes=10)


# ── fixture ─────────────────────────────────────────────────────────────────


@pytest.fixture
async def rdb():
    """独占一个空的 Redis 逻辑库;真 Redis 不可用时回落 fakeredis(不 skip)。"""
    import redis.asyncio as aioredis

    addr = os.getenv("PANDORA_TEST_REDIS_ADDR", "127.0.0.1:16379")
    host, _, port = addr.rpartition(":")

    client = None
    for i in range(16):
        db = (os.getpid() + i) % 16
        candidate = aioredis.Redis(
            host=host or "127.0.0.1",
            port=int(port or 16379),
            db=db,
            decode_responses=False,
            socket_connect_timeout=3,
            socket_timeout=3,
        )
        try:
            await asyncio.wait_for(candidate.ping(), timeout=4)
        except Exception:  # noqa: BLE001 —— 连不上/超时都回落,原因不影响决策
            await candidate.aclose()
            client = None
            break
        if await candidate.dbsize() == 0:
            client = candidate
            break
        await candidate.aclose()
    if client is None:
        from fakeredis import aioredis as fake

        client = fake.FakeRedis(decode_responses=False)
    try:
        yield client
    finally:
        await client.flushdb()
        await client.aclose()


# ── 假 apiserver ────────────────────────────────────────────────────────────


def _json_resp(status: int, payload: Any) -> httpx.Response:
    return httpx.Response(
        status,
        content=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )


def _unescape_pointer(token: str) -> str:
    """JSON Pointer 反转义。**顺序与转义相反**(先 `~1` 再 `~0`),否则 `~01` 会被
    错拆成 `/`。"""
    return token.replace("~1", "/").replace("~0", "~")


def _pointer_get(obj: dict[str, Any], pointer: str) -> Any:
    cur: Any = obj
    for raw in pointer.split("/")[1:]:
        key = _unescape_pointer(raw)
        if not isinstance(cur, dict) or key not in cur:
            return None
        cur = cur[key]
    return cur


def _pointer_add(obj: dict[str, Any], pointer: str, value: Any) -> None:
    keys = [_unescape_pointer(raw) for raw in pointer.split("/")[1:]]
    cur: Any = obj
    for key in keys[:-1]:
        cur = cur.setdefault(key, {})
    cur[keys[-1]] = value


def _selector_matches(selector: str, gs: dict[str, Any]) -> bool:
    labels = gs.get("metadata", {}).get("labels") or {}
    for term in selector.split(","):
        if term == "":
            continue
        key, _, want = term.partition("=")
        if labels.get(key) != want:
            return False
    return True


class FakeAgonesApi:
    """内存版 k8s apiserver(只实现本测试用到的动词),经 `httpx.MockTransport` 注入。

    ★ 它像真 apiserver 一样**执行** JSON Patch 的 `test` op / merge-patch 的
      `metadata.resourceVersion` 前置条件:任一条件不成立就回 409 且**不应用任何改动**。
      这是本文件"条件 PATCH"系列断言成立的前提 —— 只有服务端真的会拒,
      "生产代码把条件删了"才会表现成"数据被改了",而不是"请求体少了一行"。
    ★ 应用成功后 `resourceVersion` 自增,对应真 apiserver "写落地才推进 rv" 的语义。
    """

    def __init__(self) -> None:
        self.gameservers: dict[str, dict[str, Any]] = {}
        self.pods: dict[str, dict[str, Any]] = {}
        self.fleets: dict[str, dict[str, Any]] = {}
        #: 每次 LIST 的 labelSelector 原文(断言分轨查询用)。
        self.list_selectors: list[str] = []
        #: 每次 PATCH 的 (name, content-type, 已解码 body)。
        self.patches: list[tuple[str, str, Any]] = []
        #: 每次被前置条件拒掉的 (name, 条件路径)。
        self.conflicts: list[tuple[str, str]] = []
        self.gets: list[str] = []
        self.list_status = 200
        #: 一次性钩子:下一次 PATCH 到达前执行,用来模拟"别的副本抢先写了"。
        self.before_patch: Callable[[FakeAgonesApi], None] | None = None

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self._handle)

    def _handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        gs_list = f"/apis/agones.dev/v1/namespaces/{_NS}/gameservers"
        pod_prefix = f"/api/v1/namespaces/{_NS}/pods/"
        fleet_prefix = f"/apis/agones.dev/v1/namespaces/{_NS}/fleets/"

        if request.method == "GET" and path == gs_list:
            selector = request.url.params.get("labelSelector", "")
            self.list_selectors.append(selector)
            if self.list_status != 200:
                return httpx.Response(self.list_status, content=b"apiserver exploded")
            items = [
                gs for gs in self.gameservers.values() if _selector_matches(selector, gs)
            ]
            return _json_resp(200, {"items": items})

        if path.startswith(gs_list + "/"):
            name = urllib.parse.unquote(path[len(gs_list) + 1 :])
            if request.method == "GET":
                self.gets.append(name)
                obj = self.gameservers.get(name)
                if obj is None:
                    return _json_resp(404, {"message": "gameserver not found"})
                return _json_resp(200, obj)
            if request.method == "PATCH":
                return self._patch_gameserver(name, request)

        if path.startswith(pod_prefix):
            name = urllib.parse.unquote(path[len(pod_prefix) :])
            obj = self.pods.get(name)
            if obj is None:
                return _json_resp(404, {"message": "pod not found"})
            return _json_resp(200, obj)

        if path.startswith(fleet_prefix):
            name = urllib.parse.unquote(path[len(fleet_prefix) :])
            obj = self.fleets.get(name)
            if obj is None:
                return _json_resp(404, {"message": "fleet not found"})
            if request.method == "PATCH":
                body = json.loads(request.content)
                obj.setdefault("spec", {}).update(body.get("spec") or {})
            return _json_resp(200, obj)

        return _json_resp(404, {"message": f"unhandled {request.method} {path}"})

    def _patch_gameserver(self, name: str, request: httpx.Request) -> httpx.Response:
        if self.before_patch is not None:
            hook, self.before_patch = self.before_patch, None  # 一次性
            hook(self)
        obj = self.gameservers.get(name)
        if obj is None:
            return _json_resp(404, {"message": "gameserver not found"})
        ctype = request.headers.get("Content-Type", "")
        body = json.loads(request.content)
        self.patches.append((name, ctype, body))
        md = obj["metadata"]

        if ctype == "application/json-patch+json":
            for op in body:
                if op.get("op") != "test":
                    continue
                if _pointer_get(obj, op["path"]) != op.get("value"):
                    self.conflicts.append((name, op["path"]))
                    return _json_resp(409, {"message": "the object has been modified"})
            for op in body:
                if op.get("op") == "add":
                    _pointer_add(obj, op["path"], op["value"])
        elif ctype == "application/merge-patch+json":
            patch_md = body.get("metadata") or {}
            want_rv = patch_md.get("resourceVersion")
            if want_rv is not None and want_rv != md["resourceVersion"]:
                self.conflicts.append((name, "/metadata/resourceVersion"))
                return _json_resp(409, {"message": "the object has been modified"})
            md.setdefault("annotations", {}).update(patch_md.get("annotations") or {})
        else:
            return _json_resp(415, {"message": f"unsupported content-type {ctype!r}"})

        md["resourceVersion"] = str(int(md["resourceVersion"]) + 1)
        return _json_resp(200, obj)


def _gs(
    name: str,
    *,
    uid: str,
    rv: str = "1",
    fleet: str = "pandora-hub",
    track: str = releasetrack.STABLE,
    region: str = _REGION,
    state: str = "Ready",
    address: str = "10.0.0.1",
    port: int = 7010,
    labels: dict[str, str] | None = None,
    annotations: dict[str, str] | None = None,
    track_label: str | None = None,
    track_annotation: str | None = None,
) -> dict[str, Any]:
    """造一个 Agones GameServer 的 JSON 对象。

    `track_label` / `track_annotation` 传 `None` 表示"与 track 一致";传 `""`
    表示**该项缺失**(用于构造轨道元数据漂移)。
    """
    gs_labels = {A.FLEET_LABEL_KEY: fleet, A.REGION_LABEL_KEY: region}
    label_track = track if track_label is None else track_label
    if label_track != "":
        gs_labels[A.RELEASE_TRACK_METADATA_KEY] = label_track
    gs_labels.update(labels or {})

    gs_ann: dict[str, str] = {}
    ann_track = track if track_annotation is None else track_annotation
    if ann_track != "":
        gs_ann[A.RELEASE_TRACK_METADATA_KEY] = ann_track
    gs_ann.update(annotations or {})

    return {
        "metadata": {
            "name": name,
            "uid": uid,
            "resourceVersion": rv,
            "labels": gs_labels,
            "annotations": gs_ann,
        },
        "status": {
            "state": state,
            "address": address,
            "ports": [{"name": "default", "port": port}],
        },
    }


def _agones_cfg(
    *,
    fleet_name: str = "pandora-hub",
    canary_fleet_name: str = "",
    canary_percent: int = 0,
    canary_seed: str = "",
    advertise_host: str = "",
    autoscale_enabled: bool = False,
    default_capacity: int = 500,
) -> hconf.Config:
    return hconf.Config.model_validate(
        {
            "mode": "agones",
            "hub": {
                "default_capacity": default_capacity,
                "autoscale_enabled": autoscale_enabled,
            },
            "agones": {
                "enabled": True,
                "api_server": _API,
                "namespace": _NS,
                "fleet_name": fleet_name,
                "canary_fleet_name": canary_fleet_name,
                "canary_percent": canary_percent,
                "canary_seed": canary_seed,
                "advertise_host": advertise_host,
                "token_path": "-",  # 联调档:不带 Authorization
            },
        }
    )


# ── Model B 签发 / 验签替身 ─────────────────────────────────────────────────


class FakeHubCredAuthority:
    """Model B 凭据签发 / 验签替身。

    token 是自描述的 base64(json),"验签"就是解回去。被测契约是
    **token 与 credential tuple 必须逐字段自洽**(hash 绑定 + claims 相等),
    真 RS256 只会加一层与该契约无关的密码学噪音。
    """

    def __init__(self, *, kid: str = "kid-test", ttl_ms: int = 3_600_000) -> None:
        self.kid = kid
        self.ttl_ms = ttl_ms
        self.issued: list[hubpb.HubDSCredential] = []

    async def issue(
        self, pod: str, instance_uid: str, protocol_epoch: int
    ) -> tuple[str, hubpb.HubDSCredential]:
        gen = len(self.issued) + 1
        exp_ms = int(time.time() * 1000) + self.ttl_ms
        payload = {
            "pod": pod,
            "instance_uid": instance_uid,
            "protocol_epoch": protocol_epoch,
            "gen": gen,
            "jti": f"jti-{pod}-{gen}",
            "exp_ms": exp_ms,
            "kid": self.kid,
            "writer_epoch": WRITER,
        }
        token = base64.urlsafe_b64encode(json.dumps(payload).encode("utf-8")).decode()
        cred = hubpb.HubDSCredential(
            gen=gen,
            jti=payload["jti"],
            exp_ms=exp_ms,
            kid=self.kid,
            instance_uid=instance_uid,
            protocol_epoch=protocol_epoch,
            token_sha256=hashlib.sha256(token.encode("utf-8")).hexdigest(),
            writer_epoch=WRITER,
        )
        self.issued.append(cred)
        return token, cred

    async def verify(self, token: str) -> A.HubCredentialClaims:
        payload = json.loads(base64.urlsafe_b64decode(token.encode("utf-8")))
        return A.HubCredentialClaims(**payload)


def _modelb_provider(
    api: FakeAgonesApi, rdb: Any, **cfg_kw: Any
) -> tuple[A.AgonesHubFleetProvider, FakeHubCredAuthority]:
    provider = A.AgonesHubFleetProvider(_agones_cfg(**cfg_kw), transport=api.transport())
    authority = FakeHubCredAuthority()
    provider.set_hub_authority(
        hauth.RedisHubAuthRepo(rdb),
        authority.issue,
        authority.verify,
        _RENEW_BEFORE,
        _AUTH_TTL,
    )
    return provider, authority


# ════════════════════════════════════════════════════════════════════════════
# A. fleet.py —— 共享契约
# ════════════════════════════════════════════════════════════════════════════


def test_shard_candidate_defaults_are_fail_closed() -> None:
    """`ShardCandidate` 的令牌 / 身份字段默认必须是"不可用",不是"可用"。

    为什么重要:任何一处 `ShardCandidate(...)` 漏填 `token_ready`,默认 True 就等于
    悄悄把一台令牌不可用的 Hub 放进可分配集 —— 玩家被路由过去,所有 DS 回调被
    enforce 守卫全拒,而拓扑侧全绿。默认 False 时同样的漏填只会表现为"少一台可用",
    是**保守**方向的失败。

    ★ 变异:fleet.py `ShardCandidate.token_ready: bool = False` 改成 `= True`。
    """
    empty = F.ShardCandidate()
    assert empty.token_ready is False
    assert empty.token_gen == 0
    assert empty.token_exp_ms == 0
    assert empty.instance_uid == ""
    assert empty.protocol_epoch == 0
    assert empty.release_track == ""


def test_sticky_release_track_empty_falls_back_to_stable() -> None:
    """**持久化记录**里空 release_track 必须回落 stable(轨道字段是后加的)。

    为什么重要:滚动升级前落库的分片镜像 / 归属记录里这个字段是空的。不回落就会被判
    "轨道非法"而整条路由失败 —— 一次发布把在场老玩家全踢下线(§9 不变量 21 要求
    发布不得踢人)。

    ★ 变异:fleet.py `sticky_release_track` 的 `if track == "": return releasetrack.STABLE`
      改成 `return releasetrack.CANARY`(或删掉该分支让它落到 valid() 判非法)。
    """
    assert F.sticky_release_track("") == releasetrack.STABLE
    assert F.sticky_release_track(releasetrack.STABLE) == releasetrack.STABLE
    # 已经粘在 canary 的记录必须原样保留,不得被 cohort 策略重算。
    assert F.sticky_release_track(releasetrack.CANARY) == releasetrack.CANARY


@pytest.mark.parametrize("bad", ["prod", "Stable", " stable", "STABLE", "canary "])
def test_sticky_release_track_rejects_unknown_value(bad: str) -> None:
    """非空但未知的轨道值必须 fail-closed 抛错,**不能**兜底成 stable。

    为什么重要:"空 → stable" 是旧数据迁移规则,把它扩大成 "看不懂 → stable",
    一台已经粘在 canary 的记录会因为一次大小写 / 空格差异被甩回正式池,
    §9.21 要求的"同一玩家固定轨道"当场破功。

    ★ 构造校验:这些值都**不等于**任何合法轨道值(否则测了个寂寞)。
    ★ 变异:fleet.py `sticky_release_track` 的 `raise ValueError(...)` 改成
      `return releasetrack.STABLE`。
    """
    assert bad not in (releasetrack.STABLE, releasetrack.CANARY)
    with pytest.raises(ValueError, match=r"\Ainvalid persisted hub release_track"):
        F.sticky_release_track(bad)


@pytest.mark.parametrize(
    ("desc", "obs", "expected_uid", "want"),
    [
        (
            "同一 GameServer 还活着 —— 不可路由不等于已拆机",
            F.HubInstanceObservation(
                game_server_found=True,
                game_server_uid="gs-old",
                pod_found=True,
                pod_owner_game_server_uid="gs-old",
            ),
            "gs-old",
            False,
        ),
        (
            "GameServer 对象没了但 Pod 还挂在旧 owner 上 —— 进程可能还握着玩家",
            F.HubInstanceObservation(
                game_server_found=False,
                pod_found=True,
                pod_owner_game_server_uid="gs-old",
            ),
            "gs-old",
            False,
        ),
        (
            "GameServer 已换 uid,但 Pod 的 owner 仍是旧 uid —— 旧容器还没被回收",
            F.HubInstanceObservation(
                game_server_found=True,
                game_server_uid="gs-new",
                pod_found=True,
                pod_owner_game_server_uid="gs-old",
            ),
            "gs-old",
            False,
        ),
        (
            "GameServer 与 Pod 都不在 —— 唯一的拆机证明",
            F.HubInstanceObservation(),
            "gs-old",
            True,
        ),
        (
            "同名 Pod 已被新 GameServer 重建且 owner 精确指向新 uid",
            F.HubInstanceObservation(
                game_server_found=True,
                game_server_uid="gs-new",
                pod_found=True,
                pod_owner_game_server_uid="gs-new",
            ),
            "gs-old",
            True,
        ),
        (
            "期望 uid 为空 —— 没有「谁被拆了」这个命题",
            F.HubInstanceObservation(),
            "",
            False,
        ),
    ],
)
def test_proves_teardown_table(
    desc: str, obs: F.HubInstanceObservation, expected_uid: str, want: bool
) -> None:
    """exact UID 拆机证明的六条判据(与 Go 的表驱动用例同集)。

    为什么重要:这是 §9.22 脑裂闸的**证据侧**。判 True 意味着"旧 Hub 已经物理消失,
    可以放行新 Hub";多判一次 True 就可能在旧 DS 还握着玩家时开放第二台 DS,
    同一个玩家同时在两个大厅。所以每一条"看起来像消失了"的形态都必须留在 False 侧。

    ★ 变异:fleet.py `proves_teardown` 的
      `if self.game_server_found and self.game_server_uid == expected_game_server_uid:
       return False` 整条删掉(第 1、3 行表项立刻变红)。
    """
    assert obs.proves_teardown(expected_uid) is want, desc


def test_proves_teardown_requires_exact_pod_owner() -> None:
    """Pod 还在时,owner 必须**精确等于**当前 GameServer uid 才算旧实例被替换。

    为什么重要:owner 未知 / 畸形 / 指向第三方时,我们并不知道这个 Pod 是谁的 ——
    此时判 True 等于给任意残留发通行证。fail-closed 是唯一安全方向。

    ★ 变异:fleet.py `proves_teardown` 里
      `and self.pod_owner_game_server_uid == self.game_server_uid` 改成 `and True`。
    """
    base = dict(game_server_found=True, game_server_uid="gs-new", pod_found=True)
    # owner 未知(空串)→ 不是证据
    assert F.HubInstanceObservation(**base).proves_teardown("gs-old") is False
    # owner 指向第三方 → 不是证据
    assert (
        F.HubInstanceObservation(
            **base, pod_owner_game_server_uid="gs-someone-else"
        ).proves_teardown("gs-old")
        is False
    )
    # owner 精确指向新 uid → 才是证据
    assert (
        F.HubInstanceObservation(
            **base, pod_owner_game_server_uid="gs-new"
        ).proves_teardown("gs-old")
        is True
    )


async def test_mock_provider_topology_is_one_based_and_deterministic() -> None:
    """Mock provider 的分片编号从 **1** 起,且同 region 多次列举完全一致。

    为什么重要:①`range(n)` 会让第一个分片变成 `-0` 号、端口 base+0,与 Go 副本算出
    不同的 addr,同一份 dev 配置两栈连不同端口;②不确定的拓扑会让对账每轮都在
    建 / 删 Redis 镜像,把真实缺陷淹进噪声里。

    ★ 变异:fleet.py `MockHubFleetProvider.list_shards` 的
      `for i in range(1, self._cfg.mock_shard_count + 1)` 改成 `range(self._cfg.mock_shard_count)`。
    """
    cfg = hconf.HubConf(
        mock_shard_count=3,
        mock_hub_addr_host="127.0.0.1",
        mock_hub_port_base=7000,
        default_capacity=500,
    )
    provider = F.MockHubFleetProvider(cfg)
    shards = await provider.list_shards(_REGION)

    assert [s.pod_name for s in shards] == [
        f"pandora-hub-{_REGION}-1",
        f"pandora-hub-{_REGION}-2",
        f"pandora-hub-{_REGION}-3",
    ]
    assert [s.shard_id for s in shards] == [1, 2, 3]
    assert [s.addr for s in shards] == ["127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"]
    assert {s.release_track for s in shards} == {releasetrack.STABLE}
    assert all(s.capacity == 500 for s in shards)
    # 没有真实实例 → 绝不伪造 exact 身份(否则归属定案会拼出指向虚无的 owner)。
    assert all(s.instance_uid == "" and s.protocol_epoch == 0 for s in shards)
    assert shards == await provider.list_shards(_REGION)


@pytest.mark.parametrize("count", [0, -1, -100])
async def test_mock_provider_non_positive_count_yields_nothing(count: int) -> None:
    """`mock_shard_count <= 0` 返回空列表,不兜默认值。

    为什么重要:显式写的荒谬值不许被悄悄修正 —— 兜成默认数量会让"我明明关了 mock"
    的配置照样种出假分片。

    ★ 变异:同上把循环上界改成 `max(1, ...)` 之类的兜底。
    """
    cfg = hconf.HubConf(mock_shard_count=count, mock_hub_addr_host="h", mock_hub_port_base=1)
    assert await F.MockHubFleetProvider(cfg).list_shards(_REGION) == []


# ════════════════════════════════════════════════════════════════════════════
# B. 三个实现的协议契约必须可互换
# ════════════════════════════════════════════════════════════════════════════


async def test_provider_capability_protocols_are_exactly_as_designed(tmp_path) -> None:
    """三个 provider 都是 `HubFleetProvider`;**只有** Agones 额外是 Observer / Scaler。

    为什么重要:上层用 `isinstance` 探测可选能力。
      * Mock / Local 若实现了 Scaler(哪怕是"Get 返假副本数、Set 是 no-op"的退化版),
        `auto_scale_enabled()` 会误判可扩缩容,每轮 reconcile 对假分片跑 consolidation;
      * Local 若实现了 Observer,对账就会拿本机 provider 的观测去铸 exact UID 拆机证明,
        而它根本没有 GameServer / Pod 这两个事实源 —— 等于凭空发拆机通行证(§9.22)。

    ★ 变异:给 `LocalHubFleetProvider` 加一个 `async def get_fleet_replicas(self): return 1`
      + `async def set_fleet_replicas(self, replicas): pass`,或给它加一个
      `async def observe_shard_instance(self, pod): ...`。
    """
    mock = F.MockHubFleetProvider(hconf.HubConf(mock_shard_count=1))
    local = LF.LocalHubFleetProvider(_local_cfg(tmp_path))
    agones = A.AgonesHubFleetProvider(_agones_cfg(), transport=FakeAgonesApi().transport())
    try:
        for provider in (mock, local, agones):
            assert isinstance(provider, F.HubFleetProvider)

        assert isinstance(agones, F.HubFleetPhysicalObserver)
        assert isinstance(agones, F.HubFleetScaler)

        assert not isinstance(mock, F.HubFleetPhysicalObserver)
        assert not isinstance(mock, F.HubFleetScaler)
        assert not isinstance(local, F.HubFleetPhysicalObserver)
        assert not isinstance(local, F.HubFleetScaler)
    finally:
        await agones.aclose()


async def test_local_and_agones_shard_candidates_are_interchangeable(tmp_path, rdb) -> None:
    """local 与 agones 产出的 `ShardCandidate` 必须能被同一段消费代码同样对待。

    这条是"两个实现可互换"的正面证明:同一个 `_consume` 走两条 provider,断言的每一项
    都是上层 ensure_shards / 分配路径真正依赖的字段语义 ——

      * `region` 回显**请求的 region**(不是标签里的),否则分片会被记到别的 region 名下;
      * `addr` 是 `host:port`,`capacity > 0`,`shard_id > 0`(0 在下游被当"未设置");
      * `release_track` 必须是**合法轨道值**,不能是空串(空串只在持久化记录里合法,
        由 `sticky_release_track` 负责迁移);
      * `token_ready` 是 bool,不是 truthy 值。

    两侧唯一被允许的差异只有 exact 实例身份:local 播种(它就是那个实例),
    agones 留空(线上身份由 Model B 授权记录 promote 后投影,拓扑发现不得抢先写)。

    ★ 变异:agones_fleet.py `_list_track_shards` 组装 `ShardCandidate` 时把
      `region=region` 改成 `region=gs.metadata.labels.get(A.REGION_LABEL_KEY, "")`
      (看起来"更准",实际让请求 region 与记账 region 分叉);
      或给 agones 侧补上 `instance_uid=gs.metadata.uid`。
    """

    def _consume(cand: F.ShardCandidate) -> None:
        assert cand.pod_name != ""
        assert cand.region == _REGION
        host, sep, port = cand.addr.rpartition(":")
        assert host != "" and sep == ":" and port.isdigit()
        assert cand.capacity > 0
        assert cand.shard_id > 0
        assert releasetrack.valid(cand.release_track)
        assert isinstance(cand.token_ready, bool)

    api = FakeAgonesApi()
    api.gameservers["hub-x"] = _gs("hub-x", uid="uid-x", rv="1")
    agones, _ = _modelb_provider(api, rdb)
    local = LF.LocalHubFleetProvider(_local_cfg(tmp_path))
    local.set_ds_token_issuer(_failing_issuer(), required=False)
    try:
        agones_shards = await agones.list_shards(_REGION)
        local_shards = await local.list_shards(_REGION)
    finally:
        await agones.aclose()
        await local.close()

    assert len(agones_shards) == 1
    assert len(local_shards) == 1
    for cand in (*agones_shards, *local_shards):
        _consume(cand)

    # 唯一允许的差异:exact 实例身份只由 local 播种。
    assert agones_shards[0].instance_uid == ""
    assert agones_shards[0].protocol_epoch == 0
    assert local_shards[0].instance_uid != ""
    assert local_shards[0].protocol_epoch == 1


# ════════════════════════════════════════════════════════════════════════════
# C. local_fleet.py
# ════════════════════════════════════════════════════════════════════════════


def _local_cfg(tmp_path, *, capacity: int = 500, **overrides: Any) -> hconf.LocalHubConf:
    """造一份能构造成功的 LocalHubConf(可执行文件桩落在 tmp_path)。"""
    exe = tmp_path / "PandoraServer.exe"
    if not exe.exists():
        exe.write_bytes(b"stub")
    raw: dict[str, Any] = {
        "launcher": hconf.LAUNCHER_PACKAGED,
        "executable_path": str(exe),
        "map_name": "/Game/Maps/Hub",
        "advertise_host": "127.0.0.1",
        "port": 7777,
        "region": _REGION,
        "capacity": capacity,
    }
    raw.update(overrides)
    return hconf.LocalHubConf(**raw)


def _failing_issuer(calls: list[tuple[str, str, int]] | None = None):
    """一个恒失败的 DS 令牌签发器(依赖注入,不 patch 内部状态)。"""

    async def _issue(pod: str, instance_uid: str, protocol_epoch: int):
        if calls is not None:
            calls.append((pod, instance_uid, protocol_epoch))
        raise RuntimeError("signer down")

    return _issue


@pytest.mark.parametrize(
    ("map_name", "capacity", "want"),
    [
        ("/Game/Maps/Hub", 500, "/Game/Maps/Hub?MaxPlayers=500"),  # 缺就补
        ("/Game/Maps/Hub?MaxPlayers=500", 500, "/Game/Maps/Hub?MaxPlayers=500"),  # 一致就原样
        ("/Game/Maps/Hub?Foo=1?maxplayers=500", 500, "/Game/Maps/Hub?Foo=1?maxplayers=500"),
    ],
)
def test_hub_map_url_with_max_players_accepts(map_name: str, capacity: int, want: str) -> None:
    """MaxPlayers 缺失时补齐;已写且**逐字**等于 capacity 时原样返回(键名大小写不敏感)。

    为什么重要:UE 的 MaxPlayers 决定 DS 实际接多少人,Redis 容量账本按 capacity 记账。
    两者不等的后果是**没有任何一侧报错**:账本说还有座,DS 直接拒连,玩家反复
    "进大厅失败"而全链日志绿的。

    ★ 变异:local_fleet.py `hub_map_url_with_max_players` 末尾
      `return f"{map_name}?MaxPlayers={capacity}"` 改成 `return map_name`(第 1 行变红);
      或把 `key_value[0].strip().lower() != "maxplayers"` 改成大小写敏感比较(第 3 行变红)。
    """
    assert LF.hub_map_url_with_max_players(map_name, capacity) == want


@pytest.mark.parametrize(
    ("map_name", "capacity", "why"),
    [
        ("/Game/Maps/Hub?MaxPlayers=400", 500, "数值不等"),
        ("/Game/Maps/Hub?MaxPlayers=500?MaxPlayers=500", 500, "重复项"),
        ("/Game/Maps/Hub?MaxPlayers", 500, "缺 = 号"),
        ("/Game/Maps/Hub?MaxPlayers=0500", 500, "非规范前导零"),
        ("/Game/Maps/Hub?MaxPlayers=+500", 500, "非规范正号"),
        ("/Game/Maps/Hub?MaxPlayers=abc", 500, "非数字"),
        ("/Game/Maps/Hub", 0, "capacity 非正"),
    ],
)
def test_hub_map_url_with_max_players_rejects(map_name: str, capacity: int, why: str) -> None:
    """任何"看起来差不多"的 MaxPlayers 都必须启动即 fail-fast。

    为什么连 `"0500"` / `"+500"` 也拒:它们解析出来确实等于 500,但说明 yaml 是人手改的、
    且与 capacity 字段两处独立维护 —— 这次碰巧一致,下次改 capacity 时必然漏改一处。

    ★ 构造校验:非规范串必须**与规范串字节不同**(否则等于什么都没测)。
    ★ 变异:local_fleet.py 里 `or key_value[1] != canonical` 整段删掉
      (前导零 / 正号两行立刻变红);把 `if found or len(key_value) != 2` 的 `found or`
      删掉(重复项那行变红)。
    """
    if capacity > 0 and "=" in map_name:
        written = map_name.rsplit("=", 1)[1]
        assert written != str(capacity) or map_name.count("MaxPlayers") > 1, why
    with pytest.raises(ValueError):
        LF.hub_map_url_with_max_players(map_name, capacity)


@pytest.mark.parametrize(
    "key",
    [
        "PANDORA_DS_TOKEN",
        "pandora_ds_token",
        "Pandora_Ds_Token",
        "  PANDORA_DS_TOKEN  ",
        "AGONES_GAMESERVER_NAME",
        "pandora_ds_type",
        "pandora_region",
        "pandora_ds_local_profile",
    ],
)
def test_is_reserved_hub_ds_env_key_is_case_insensitive(key: str) -> None:
    """保留 env key 的判定必须**大小写不敏感 + 先 strip**。

    为什么重要:Windows(local 模式 Hub DS 的宿主)环境变量名大小写不敏感,
    小写别名 `pandora_ds_token` 与内置大写名指向同一个变量。精确比对会放行小写覆盖,
    等于让 extra_env 用一个静态 / 伪造令牌顶掉真签发令牌,绕过令牌的范围绑定。

    ★ 变异:local_fleet.py `is_reserved_hub_ds_env_key` 的
      `key.strip().upper() in RESERVED_HUB_DS_ENV_KEYS` 改成 `key in RESERVED_HUB_DS_ENV_KEYS`。
    """
    assert LF.is_reserved_hub_ds_env_key(key) is True


def test_is_reserved_hub_ds_env_key_lets_normal_keys_through() -> None:
    """普通 key 必须放行,否则 extra_env 整体失效(反向护栏)。"""
    assert LF.is_reserved_hub_ds_env_key("PANDORA_LOG_LEVEL") is False
    assert LF.is_reserved_hub_ds_env_key("") is False


def test_local_provider_constructor_fails_fast(tmp_path) -> None:
    """构造期就必须拒掉的四类配置错误。

    为什么重要:这些错误在运行期的表现全都是"Hub DS 秒退 / 根本没起来",而
    hub_allocator 自己一切正常 —— 玩家登录后连不上大厅,日志里没有任何直接线索。
    启动即 fail-fast 才能把问题钉在配置上。

    ★ 变异:local_fleet.py `__init__` 里 `if not os.path.exists(cfg.executable_path)`
      整条删掉;或把 editor 分支的 `if cfg.project_path == ""` 删掉。
    """
    with pytest.raises(ValueError, match=r"executable_path required"):
        LF.LocalHubFleetProvider(_local_cfg(tmp_path, executable_path=""))

    missing = tmp_path / "nope" / "PandoraServer.exe"
    assert not missing.exists()
    with pytest.raises(ValueError, match=r"not found"):
        LF.LocalHubFleetProvider(_local_cfg(tmp_path, executable_path=str(missing)))

    with pytest.raises(ValueError, match=r"project_path required"):
        LF.LocalHubFleetProvider(
            _local_cfg(tmp_path, launcher=hconf.LAUNCHER_EDITOR, project_path="")
        )

    # MaxPlayers 与 capacity 不一致 → 构造期就拒(不是等到玩家连不上才发现)。
    with pytest.raises(ValueError, match=r"MaxPlayers must exactly equal capacity"):
        LF.LocalHubFleetProvider(
            _local_cfg(tmp_path, map_name="/Game/Maps/Hub?MaxPlayers=400", capacity=500)
        )


async def test_local_fleet_enforce_fails_closed_and_never_retries(tmp_path) -> None:
    """enforce 下签发失败 → `list_shards` 抛 `ErrHubNoAvailable`,**且不重试**。

    为什么重要(两条):
      ① 返回候选会让 ensure_shards 在 Redis 种一条 ready 记录,把客户端路由到一个
         根本没拉起 / 回调必被守卫全拒的 Hub —— 玩家进得去大厅、什么也做不了。
      ② 不重试是 Go `sync.Once` 的语义:UE 可执行文件路径配错这种必然失败的场景,
         每轮 reconcile 重试一次就会在几分钟内攒出成百个僵尸 UE 进程,把开发机拖死。

    ★ 变异:local_fleet.py `list_shards` 的
      `if self._ds_token_required and self._start_err is not None: raise ...` 删掉(①变红);
      `_ensure_started` 里失败后加 `self._started = False`(②变红)。
    """
    calls: list[tuple[str, str, int]] = []
    provider = LF.LocalHubFleetProvider(_local_cfg(tmp_path))
    provider.set_ds_token_issuer(_failing_issuer(calls), required=True)
    try:
        for _ in range(3):
            with pytest.raises(errcode.PandoraError) as ei:
                await provider.list_shards(_REGION)
            assert ei.value.code == errcode.ErrHubNoAvailable
    finally:
        await provider.close()

    # 签发器只被调用一次:失败不重试(否则是僵尸进程工厂)。
    assert len(calls) == 1
    assert calls[0][0] == provider.pod_name
    # 签发失败 → 没有可回显的 ACK 身份。
    assert provider.local_credential_ack(provider.pod_name) is None


async def test_local_fleet_permissive_still_returns_candidate(tmp_path) -> None:
    """off / permissive 下启动失败**不**影响可用性,仍返回唯一候选分片。

    为什么重要:本机联调时 UE 还没编译出来是常态,此时把整个 hub_allocator 卡死
    没有任何收益;客户端连不上会自己报错,反而更好排查。fail-closed 只属于 enforce。

    ★ 变异:local_fleet.py `list_shards` 的 fail-closed 判据把 `self._ds_token_required and`
      去掉(permissive 也开始抛错 → 本条变红)。
    """
    provider = LF.LocalHubFleetProvider(_local_cfg(tmp_path))
    provider.set_ds_token_issuer(_failing_issuer(), required=False)
    try:
        shards = await provider.list_shards(_REGION)
    finally:
        await provider.close()

    assert len(shards) == 1
    assert shards[0].pod_name == provider.pod_name
    assert shards[0].shard_id == 1
    assert shards[0].release_track == releasetrack.STABLE
    # 签发被跳过 → gen 回落 0(与旧行为一致),但分片本身仍可用。
    assert shards[0].token_gen == 0


async def test_local_build_env_carries_model_b_identity_and_local_profile(tmp_path) -> None:
    """下发给 UE Hub DS 的 env 必须携带 exact 实例身份 + 令牌 + local-off 档位。

    这是 hub_allocator 与 UE DS 之间**唯一**的进程级契约(local 模式没有 annotation
    可续期),所以直接断言它的产物。任一项漏掉的表现都是"DS 起来了但所有回调被拒"
    或"心跳 ACK 永远 mismatched",而两侧日志各自正常。

    ★ 变异:local_fleet.py `_build_env` 里
      `env[hconf.DS_LOCAL_PROFILE_ENV] = hconf.DS_LOCAL_PROFILE_OFF_V1` 删掉;
      或把签发器调用参数 `self._instance_uid, self._protocol_epoch` 换成空串 / 0
      (`local_credential_ack` 的逐字段回显立刻对不上)。
    """
    seen: list[tuple[str, str, int]] = []

    async def _issue(pod: str, instance_uid: str, protocol_epoch: int):
        seen.append((pod, instance_uid, protocol_epoch))
        return "tok-local", LF.LocalHubCredential(
            instance_uid=instance_uid,
            protocol_epoch=protocol_epoch,
            gen=7,
            jti="jti-local",
            writer_epoch=WRITER,
            expires_at_ms=int(time.time() * 1000) + 600_000,
        )

    provider = LF.LocalHubFleetProvider(_local_cfg(tmp_path))
    provider.set_ds_token_issuer(_issue, required=True)
    env = await provider._build_env()  # noqa: SLF001 —— env 就是被测的进程级契约

    assert env[LF.GAMESERVER_NAME_ENV] == provider.pod_name
    assert env[LF.DS_TYPE_ENV] == "hub"
    assert env[LF.REGION_ENV] == _REGION
    assert env[hconf.DS_LOCAL_PROFILE_ENV] == hconf.DS_LOCAL_PROFILE_OFF_V1
    assert env[LF.DS_TOKEN_ENV] == "tok-local"

    assert len(seen) == 1
    pod, instance_uid, epoch = seen[0]
    assert pod == provider.pod_name
    assert epoch == 1
    assert instance_uid != ""

    # ACK 身份与 env 里的 token 同源:心跳应答就是拿它逐字段回显的。
    ack = provider.local_credential_ack(provider.pod_name)
    assert ack is not None
    assert (ack.instance_uid, ack.protocol_epoch, ack.gen, ack.jti, ack.writer_epoch) == (
        instance_uid,
        1,
        7,
        "jti-local",
        WRITER,
    )
    # 别的 pod 名一律不回显 —— 绝不能拿别人的身份糊一个 ACK 出去。
    assert provider.local_credential_ack(provider.pod_name + "-other") is None
    assert provider.local_credential_ack("") is None


async def test_local_extra_env_cannot_override_reserved_keys(tmp_path) -> None:
    """`extra_env` 不得覆盖内置身份 / 令牌变量 —— 含**小写别名**。

    为什么重要:extra_env 覆盖 `PANDORA_DS_TOKEN` = 用静态 / 伪造令牌替换真签发令牌,
    绕过令牌的范围绑定;覆盖 `PANDORA_DS_LOCAL_PROFILE` = 把本机隔离档位关掉。
    Windows env 名大小写不敏感,所以小写别名是同一个变量。

    ★ 变异:local_fleet.py `_build_env` 的 extra_env 循环里
      `if is_reserved_hub_ds_env_key(k): continue` 删掉。
    """
    provider = LF.LocalHubFleetProvider(
        _local_cfg(
            tmp_path,
            extra_env={
                "pandora_ds_local_profile": "hijacked",
                "PANDORA_DS_TOKEN": "fake-token",
                "AGONES_GAMESERVER_NAME": "someone-else",
                "PANDORA_LOG_LEVEL": "debug",  # 正常 key:必须放行
            },
        )
    )

    async def _issue(pod: str, instance_uid: str, protocol_epoch: int):
        return "tok-real", LF.LocalHubCredential(
            instance_uid=instance_uid,
            protocol_epoch=protocol_epoch,
            gen=1,
            jti="jti-1",
            writer_epoch=WRITER,
            expires_at_ms=int(time.time() * 1000) + 600_000,
        )

    provider.set_ds_token_issuer(_issue, required=True)
    env = await provider._build_env()  # noqa: SLF001

    assert env[hconf.DS_LOCAL_PROFILE_ENV] == hconf.DS_LOCAL_PROFILE_OFF_V1
    assert env[LF.DS_TOKEN_ENV] == "tok-real"
    assert env[LF.GAMESERVER_NAME_ENV] == provider.pod_name
    assert env["PANDORA_LOG_LEVEL"] == "debug"


def test_local_build_args_order(tmp_path) -> None:
    """UE 命令行顺序:`[.uproject] 关卡URL -server -log -port=N [CVar] extra_args`。

    为什么重要:UE 的 `LaunchSetGameName` 只把命令行里**第一个**不以 `-` 开头的 token
    当工程 / 关卡。`.uproject` 排在关卡 URL 之后就会被当成关卡名解析失败,
    表现为 Hub DS 秒退。CVar 必须排在 extra_args **之前**,运维才能用 extra_args 覆盖回去。

    ★ 变异:local_fleet.py `_build_args` 里把 `.uproject` 的 append 挪到 map_url 之后;
      或把 `args.append(hconf.EDITOR_LAUNCHER_CVAR_ARG)` 挪到 `args.extend(extra_args)` 之后。
    """
    packaged = LF.LocalHubFleetProvider(_local_cfg(tmp_path, extra_args=["-nosound"]))
    assert packaged._build_args() == [  # noqa: SLF001 —— 命令行就是被测的进程级契约
        "/Game/Maps/Hub?MaxPlayers=500",
        "-server",
        "-log",
        "-port=7777",
        "-nosound",
    ]
    # packaged 形态**不得**带 editor 专用 CVar(它只为未 cook 工程存在)。
    assert hconf.EDITOR_LAUNCHER_CVAR_ARG not in packaged._build_args()  # noqa: SLF001

    uproject = tmp_path / "Pandora.uproject"
    uproject.write_text("{}", encoding="utf-8")
    unreal_editor = tmp_path / "UnrealEditor.exe"
    unreal_editor.write_bytes(b"stub")
    editor = LF.LocalHubFleetProvider(
        _local_cfg(
            tmp_path,
            launcher=hconf.LAUNCHER_EDITOR,
            executable_path=str(unreal_editor),
            project_path=str(uproject),
            extra_args=["-nosound"],
        )
    )
    args = editor._build_args()  # noqa: SLF001
    assert args[0] == str(uproject)  # .uproject 必须在最前
    assert args[1] == "/Game/Maps/Hub?MaxPlayers=500"
    assert args[2:5] == ["-server", "-log", "-port=7777"]
    assert args[5] == hconf.EDITOR_LAUNCHER_CVAR_ARG  # CVar 在 extra_args 之前
    assert args[6] == "-nosound"
    # CVar 必须是单个 token(含空格 / 引号会被 UE 拆成两个参数,静默失效)。
    assert not any(ch in hconf.EDITOR_LAUNCHER_CVAR_ARG for ch in (" ", "\t", '"', "'"))
    assert "net.SkipMissingLevelDisconnect=1" in hconf.EDITOR_LAUNCHER_CVAR_ARG


# ════════════════════════════════════════════════════════════════════════════
# D. agones_fleet.py —— 拓扑发现与轨道
# ════════════════════════════════════════════════════════════════════════════


def test_agones_constructor_guards() -> None:
    """构造期就必须拒掉的四类 Agones 配置。

    为什么重要:`canary_percent > 0` 却没有 canary Fleet,会让被判进 canary 的玩家
    永远找不到分片(§9.19 玩家卡死);双轨 Fleet 同时开 autoscale,scaler 只治理 stable
    Fleet,却拿两轨总负载算副本数 —— 一次灰度就把正式池扩成两倍。

    ★ 变异:agones_fleet.py `__init__` 里
      `if ag.canary_percent > 0 and ag.canary_fleet_name.strip() == "": raise` 删掉;
      或删掉 autoscale + 双轨的互斥判据。
    """
    with pytest.raises(ValueError, match=r"fleet_name required"):
        A.AgonesHubFleetProvider(_agones_cfg(fleet_name=""))
    with pytest.raises(ValueError, match=r"invalid canary policy"):
        A.AgonesHubFleetProvider(_agones_cfg(canary_percent=150, canary_seed="s"))
    with pytest.raises(ValueError, match=r"canary_fleet_name required"):
        A.AgonesHubFleetProvider(_agones_cfg(canary_percent=10, canary_seed="s"))
    with pytest.raises(ValueError, match=r"autoscale is not supported"):
        A.AgonesHubFleetProvider(
            _agones_cfg(canary_fleet_name="pandora-hub-canary", autoscale_enabled=True)
        )


async def test_agones_list_shards_selector_and_filtering() -> None:
    """LIST 的 labelSelector 三项齐全;未就绪的 GameServer 一律不进拓扑。

    为什么重要:selector 少一项 = 把别的 Fleet / 别的 region 的 Hub 当成自己的候选;
    Scheduled / 无 address 的 GameServer 进了拓扑 = 玩家被路由到一个还没有网络地址的
    Pod,客户端连接超时。

    ★ 变异:agones_fleet.py `_list_track_shards` 把
      `if region != "": selector += f",{REGION_LABEL_KEY}={region}"` 删掉;
      或把 `if gs.status.address == "" or not gs.status.ports: continue` 删掉。
    """
    api = FakeAgonesApi()
    api.gameservers["hub-0"] = _gs(
        "hub-0",
        uid="uid-0",
        port=7010,
        labels={A.SHARD_ID_LABEL_KEY: "7", A.CAPACITY_LABEL_KEY: "300"},
    )
    api.gameservers["hub-1"] = _gs("hub-1", uid="uid-1", port=7011, state="Allocated")
    api.gameservers["hub-2"] = _gs("hub-2", uid="uid-2", port=7012, state="Scheduled")
    api.gameservers["hub-3"] = _gs("hub-3", uid="uid-3", port=7013, address="")

    provider = A.AgonesHubFleetProvider(_agones_cfg(), transport=api.transport())
    try:
        shards = await provider.list_shards(_REGION)
    finally:
        await provider.aclose()

    assert api.list_selectors == [
        f"{A.FLEET_LABEL_KEY}=pandora-hub"
        f",{A.RELEASE_TRACK_METADATA_KEY}={releasetrack.STABLE}"
        f",{A.REGION_LABEL_KEY}={_REGION}"
    ]
    assert [s.pod_name for s in shards] == ["hub-0", "hub-1"]

    assert shards[0].shard_id == 7  # 标签优先
    assert shards[0].capacity == 300  # 标签优先
    assert shards[0].addr == "10.0.0.1:7010"
    assert shards[0].release_track == releasetrack.STABLE
    assert shards[0].token_ready is True  # 未注入签发器 → 令牌路径 no-op

    assert shards[1].shard_id != 0  # 缺标签 → 按 pod 名哈希,且永不为 0
    assert shards[1].capacity == 500  # 缺标签 → 回落 default_capacity
    # exact 实例身份**不由拓扑发现写**(线上身份属于 Model B 授权记录)。
    assert all(s.instance_uid == "" and s.protocol_epoch == 0 for s in shards)


async def test_agones_list_shards_without_region_omits_region_selector() -> None:
    """region 为空时 selector 不带 region 项,且候选的 region 原样回显空串。

    为什么重要:塞一个空的 `pandora.dev/region=` 进 selector 会匹配不到任何
    GameServer(真 apiserver 按空值精确匹配),表现为"全 region 一台 Hub 都没有"。

    ★ 变异:agones_fleet.py `_list_track_shards` 把 `if region != "":` 判据删掉,
      改成无条件拼接。
    """
    api = FakeAgonesApi()
    api.gameservers["hub-any"] = _gs("hub-any", uid="uid-any")
    provider = A.AgonesHubFleetProvider(_agones_cfg(), transport=api.transport())
    try:
        shards = await provider.list_shards("")
    finally:
        await provider.aclose()

    assert api.list_selectors == [
        f"{A.FLEET_LABEL_KEY}=pandora-hub"
        f",{A.RELEASE_TRACK_METADATA_KEY}={releasetrack.STABLE}"
    ]
    assert A.REGION_LABEL_KEY not in api.list_selectors[0]
    assert len(shards) == 1
    assert shards[0].region == ""


async def test_agones_dual_fleet_keeps_stable_and_canary_separate() -> None:
    """双轨 Fleet:两次独立 LIST,轨道由 GameServer 元数据证明,顺序 stable→canary。

    为什么重要(§9 不变量 21):同一玩家 / 对局必须固定 release track。轨道一旦由
    "查的是哪个 Fleet"推断而不是由 GameServer 元数据证明,一台错标的 canary 实例
    就会作为 stable 候选被分配出去 —— 灰度实例混进正式池,而且**没有任何报错**。

    ★ 变异:agones_fleet.py `_list_track_shards` 里 `release_track=release_track`
      改成 `release_track=releasetrack.STABLE`(canary 分片被打成 stable → 变红);
      或把 `list_shards` 的 canary 分支删掉。
    """
    api = FakeAgonesApi()
    api.gameservers["hub-s"] = _gs("hub-s", uid="uid-s", fleet="pandora-hub", port=7010)
    api.gameservers["hub-c"] = _gs(
        "hub-c",
        uid="uid-c",
        fleet="pandora-hub-canary",
        track=releasetrack.CANARY,
        port=7020,
    )
    provider = A.AgonesHubFleetProvider(
        _agones_cfg(canary_fleet_name="pandora-hub-canary", canary_percent=10, canary_seed="s"),
        transport=api.transport(),
    )
    try:
        shards = await provider.list_shards(_REGION)
    finally:
        await provider.aclose()

    assert len(api.list_selectors) == 2
    assert f"{A.FLEET_LABEL_KEY}=pandora-hub," in api.list_selectors[0]
    assert f"{A.RELEASE_TRACK_METADATA_KEY}={releasetrack.STABLE}" in api.list_selectors[0]
    assert f"{A.FLEET_LABEL_KEY}=pandora-hub-canary," in api.list_selectors[1]
    assert f"{A.RELEASE_TRACK_METADATA_KEY}={releasetrack.CANARY}" in api.list_selectors[1]

    assert [(s.pod_name, s.release_track) for s in shards] == [
        ("hub-s", releasetrack.STABLE),
        ("hub-c", releasetrack.CANARY),
    ]


@pytest.mark.parametrize(
    ("desc", "kw"),
    [
        ("release-track 标签缺失", {"track_label": ""}),
        ("release-track annotation 缺失", {"track_annotation": ""}),
        ("标签写着 canary(与查询轨道不符)", {"track_label": releasetrack.CANARY}),
        ("annotation 写着 canary(与标签不符)", {"track_annotation": releasetrack.CANARY}),
        ("fleet 标签指向别的 Fleet", {"fleet": "pandora-hub-canary"}),
    ],
)
async def test_agones_never_defaults_missing_track_to_stable(desc: str, kw: Any) -> None:
    """拓扑发现对轨道元数据是**严格相等**:缺失 / 不一致一律跳过,**绝不**兜成 stable。

    这是 `sticky_release_track("") == stable` 的**反方向**,两处方向相反是分工不是笔误:
    持久化记录有"滚动升级前落库时字段还不存在"的历史,GameServer 是编排层的实时对象,
    没有历史这回事。把一台没打轨道标签的 canary GameServer 默认成 stable,
    等于把灰度实例混进正式池(§9.21 明令禁止)。

    ★ 构造校验:每种形态都确实与"合法 stable 元数据"不同(见 assert)。
    ★ 变异:agones_fleet.py `_list_track_shards` 的三重严格相等判据里,
      把 `gs.metadata.ann(RELEASE_TRACK_METADATA_KEY) != release_track` 一项删掉
      (前两行 + 第 4 行变红);或整块 `continue` 改成 `release_track = releasetrack.STABLE`。
    """
    api = FakeAgonesApi()
    drifted = _gs("hub-drift", uid="uid-drift", **kw)
    legal = _gs("hub-legal", uid="uid-legal", port=7099)
    assert drifted["metadata"]["labels"] != legal["metadata"]["labels"] or drifted[
        "metadata"
    ]["annotations"] != legal["metadata"]["annotations"], f"{desc}:构造出来的元数据没漂移"
    api.gameservers["hub-drift"] = drifted
    api.gameservers["hub-legal"] = legal

    provider = A.AgonesHubFleetProvider(_agones_cfg(), transport=api.transport())
    try:
        shards = await provider.list_shards(_REGION)
    finally:
        await provider.aclose()

    # 只剩合法那台;漂移那台既没被跳过成"消失",也没被兜成 stable。
    assert [s.pod_name for s in shards] == ["hub-legal"]
    assert shards[0].release_track == releasetrack.STABLE


async def test_agones_list_shards_server_error_is_not_an_empty_fleet() -> None:
    """apiserver 5xx 必须抛错,**不能**降级成"Fleet 里一台也没有"。

    为什么重要:空列表在上层是"该清理镜像"的信号。把一次 apiserver 抖动翻译成空 Fleet,
    对账会把所有还活着的 Hub 分片记录删掉,在场玩家整体失去路由(§9.22:查询失败
    返回 UNKNOWN / UNAVAILABLE,不得冒充空/默认状态)。

    ★ 变异:agones_fleet.py `_list_track_shards` 的
      `if not _is_2xx(resp.status): raise AgonesError(...)` 改成 `return []`。
    """
    api = FakeAgonesApi()
    api.gameservers["hub-a"] = _gs("hub-a", uid="uid-a")
    api.list_status = 503
    provider = A.AgonesHubFleetProvider(_agones_cfg(), transport=api.transport())
    try:
        with pytest.raises(A.AgonesError, match=r"http 503"):
            await provider.list_shards(_REGION)
    finally:
        await provider.aclose()


def test_shard_id_and_capacity_label_override_and_stability() -> None:
    """shard_id 标签优先、非法回落哈希且哈希**跨进程稳定**;capacity 同理回落。

    为什么重要:`shard_id` 会被写进 Redis 分片记录。用 Python 内建 `hash()` 会因每进程
    加盐而让同一个 pod 名在两次进程里算出不同 shard_id —— 重启一次全表漂移。
    哈希为 0 时抬成 1,因为 0 在下游被当作"未设置"。

    ★ 变异:agones_fleet.py `_fnv1a32` 改成 `return hash(data) & 0xFFFFFFFF`
      (固定期望值那条变红);`shard_id_for` 的 `if 0 < n <= 0xFFFFFFFF` 判据删掉
      (非法标签回落那条变红)。
    """
    labeled = A.GameServer(
        metadata=A.GSMetadata(name="hub-x", labels={A.SHARD_ID_LABEL_KEY: "42"})
    )
    assert A.shard_id_for(labeled) == 42

    # 非法 / 越界标签 → 回落 pod 名哈希(不是回落 0,也不是沿用非法值)。
    for bad in ("0", "-1", "abc", str(0xFFFFFFFF + 1)):
        gs = A.GameServer(metadata=A.GSMetadata(name="hub-x", labels={A.SHARD_ID_LABEL_KEY: bad}))
        assert A.shard_id_for(gs) == A.shard_id_for(A.GameServer(metadata=A.GSMetadata(name="hub-x")))

    # 用 FNV-1a 32 的**官方测试向量**钉死算法本身(不是照抄实现算出来的值):
    # http://www.isthe.com/chongo/tech/comp/fnv/ 的 "a" / "foobar" 标准结果。
    # 这样"换成 Python 内建 hash() / 换个种子"这类改动一定被抓,而不是被测试跟着改。
    assert A._fnv1a32(b"a") == 0xE40C292C  # noqa: SLF001
    assert A._fnv1a32(b"foobar") == 0xBF9CF968  # noqa: SLF001
    hub_x = A.shard_id_for(A.GameServer(metadata=A.GSMetadata(name="hub-x")))
    assert hub_x == A._fnv1a32(b"hub-x")  # noqa: SLF001 —— 哈希的输入是 pod 名的 utf-8 字节
    assert A.shard_id_for(A.GameServer(metadata=A.GSMetadata(name="hub-y"))) != hub_x

    assert A.capacity_for(A.GameServer(), 500) == 500
    assert (
        A.capacity_for(
            A.GameServer(metadata=A.GSMetadata(labels={A.CAPACITY_LABEL_KEY: "300"})), 500
        )
        == 300
    )
    for bad in ("0", "-5", "nope"):
        gs = A.GameServer(metadata=A.GSMetadata(labels={A.CAPACITY_LABEL_KEY: bad}))
        assert A.capacity_for(gs, 500) == 500


async def test_agones_observe_ignores_routability_and_reads_pod_owner() -> None:
    """物理存活观测**刻意不看** readiness / health / 轨道 / 令牌。

    为什么重要:那些是**路由事实**,不是**物理存活事实**。用它们过滤会让一台
    Unhealthy 但进程还活着、还握着玩家的 Hub 被判成"已拆机",于是新 Hub 被放行 ——
    §9.22 的脑裂。

    ★ 变异:agones_fleet.py `observe_shard_instance` 里加一句
      `if gs.status.state not in HUB_READY_STATES: return obs`(第一段断言变红);
      或把 owner 循环的 `if owner.kind == "GameServer"` 改成无条件取第一个 owner。
    """
    api = FakeAgonesApi()
    api.gameservers["hub-sick"] = _gs("hub-sick", uid="uid-sick", state="Unhealthy", address="")
    api.pods["hub-sick"] = {
        "metadata": {
            "name": "hub-sick",
            "uid": "pod-uid",
            "ownerReferences": [
                {"kind": "ReplicaSet", "uid": "rs-uid"},  # 必须被跳过
                {"kind": "GameServer", "uid": "uid-sick"},
            ],
        }
    }
    provider = A.AgonesHubFleetProvider(_agones_cfg(), transport=api.transport())
    try:
        obs = await provider.observe_shard_instance("hub-sick")
        # 该实例在 list_shards 里根本不出现(不可路由),但观测必须证明它还活着。
        assert await provider.list_shards(_REGION) == []
        gone = await provider.observe_shard_instance("hub-ghost")
    finally:
        await provider.aclose()

    assert obs.game_server_found is True
    assert obs.game_server_uid == "uid-sick"
    assert obs.pod_found is True
    assert obs.pod_owner_game_server_uid == "uid-sick"
    assert obs.proves_teardown("uid-sick") is False

    # GameServer 与 Pod 都 404 → 这才是拆机证明。
    assert gone.game_server_found is False
    assert gone.pod_found is False
    assert gone.proves_teardown("uid-sick") is True


def test_escape_json_pointer_escapes_tilde_before_slash() -> None:
    """JSON Pointer 转义顺序必须先 `~` 后 `/`。

    为什么重要:Model B 的 annotation key 形如 `pandora.dev/ds-token`,每一项都要转义成
    Pointer 路径。顺序反了会产出 `~01` 这种能被服务端反转义成别的路径的串 ——
    PATCH 打到错误的字段上,而 HTTP 照样 2xx。

    ★ 变异:agones_fleet.py `escape_json_pointer` 把两个 `replace` 调换顺序。
    """
    assert A.escape_json_pointer(A.DS_TOKEN_ANNOTATION_KEY) == "pandora.dev~1ds-token"
    assert A.escape_json_pointer("a~b/c") == "a~0b~1c"
    assert _unescape_pointer(A.escape_json_pointer("a~b/c")) == "a~b/c"


# ════════════════════════════════════════════════════════════════════════════
# E. Model B 凭据投递 —— resourceVersion / uid 条件 PATCH
# ════════════════════════════════════════════════════════════════════════════


async def test_modelb_patch_binds_uid_and_resource_version(rdb) -> None:
    """凭据 PATCH 的前两个 op 必须是 `/metadata/uid` 与 `/metadata/resourceVersion` 的 test。

    为什么重要:这两条 `test` 就是 Agones 侧的乐观并发闸。少了 uid test,同名 Pod 被
    重建后旧投递会打到新实例上;少了 rv test,多副本交错写会互相覆盖 —— 两种情况都
    让 K8s annotation 与 Redis 授权记录分叉,DS 拿着一份谁也不认的令牌。

    另一条同样关键:**判定成功的唯一标准是 rv 前进了**。apiserver 对内容完全相同的
    PATCH 也回 2xx 但不推进 rv,那说明我们的写根本没落地。

    ★ 变异:agones_fleet.py `_patch_credential_annotation` 的 `ops` 里删掉
      `{"op": "test", "path": "/metadata/resourceVersion", ...}` 那一项;
      或把 `if confirm_err is None and rv != md.resource_version` 改成
      `if confirm_err is None`。
    """
    api = FakeAgonesApi()
    api.gameservers["hub-a"] = _gs("hub-a", uid="uid-A", rv="1")
    provider, authority = _modelb_provider(api, rdb)
    try:
        shards = await provider.list_shards(_REGION)
    finally:
        await provider.aclose()

    assert len(shards) == 1
    assert shards[0].token_ready is True
    assert shards[0].token_gen == 1
    assert shards[0].token_exp_ms == authority.issued[0].exp_ms

    assert len(api.patches) == 1
    name, ctype, ops = api.patches[0]
    assert (name, ctype) == ("hub-a", "application/json-patch+json")
    assert ops[0] == {"op": "test", "path": "/metadata/uid", "value": "uid-A"}
    assert ops[1] == {"op": "test", "path": "/metadata/resourceVersion", "value": "1"}
    assert [op["op"] for op in ops[2:]] == ["add"] * len(A.DS_CREDENTIAL_ANNOTATION_KEYS)

    final_rv = api.gameservers["hub-a"]["metadata"]["resourceVersion"]
    assert final_rv != "1"  # rv 必须真的前进
    anns = api.gameservers["hub-a"]["metadata"]["annotations"]
    assert anns == {
        A.RELEASE_TRACK_METADATA_KEY: releasetrack.STABLE,
        **A.hub_credential_annotations(_token_of(anns), authority.issued[0]),
    }

    rec, found = await hauth.RedisHubAuthRepo(rdb).get_auth("hub-a")
    assert found
    assert rec.delivered_rv == final_rv  # 记的是**确认后的**真实 rv,不是本地猜的
    assert A.credential_fields_equal(rec.pending, authority.issued[0])


def _token_of(annotations: dict[str, str]) -> str:
    return annotations[A.DS_TOKEN_ANNOTATION_KEY]


async def test_modelb_stale_resource_version_patch_fails_closed(rdb) -> None:
    """别的副本抢先改了 GameServer(rv 前进)后,本次条件 PATCH 必须被拒且**一个字节都不落地**。

    这是本文件最核心的一条。没有这道闸,两个 hub_allocator 副本同时对账同一台 Hub 时,
    后到的低代际 PATCH 会盖掉先到的高代际凭据;Redis 授权记录里 pending 是新的、
    K8s annotation 上是旧的,DS 拿到的令牌永远激活不了 —— 而 HTTP 全是 2xx。

    fail-closed 的三个具体要求都在断言里:
      ① 令牌 annotation 一个都不许写上去;
      ② 该分片仍出现在拓扑里(供对账区分"Fleet 里没有" vs "有但令牌不可用"),
         但 `token_ready=False`,不进可分配集;
      ③ Redis 的 pending 保持 `delivered_rv == ""` —— 下轮对账会重投,不会误以为已送达。

    ★ 构造校验:先断言"漂移前的 rv"确实不等于漂移后的值(否则这条测了个寂寞)。
    ★ 变异:agones_fleet.py `_patch_credential_annotation` 的 `ops` 里删掉
      `{"op": "test", "path": "/metadata/resourceVersion", ...}` —— 假 apiserver 会照单全收,
      token_ready 变 True、annotation 被写上,本条立刻变红。
    """
    api = FakeAgonesApi()
    api.gameservers["hub-b"] = _gs("hub-b", uid="uid-B", rv="1")
    captured: dict[str, str] = {}

    def _peer_wrote_first(fake: FakeAgonesApi) -> None:
        md = fake.gameservers["hub-b"]["metadata"]
        captured["before"] = md["resourceVersion"]
        md["annotations"]["pandora.dev/peer-touched"] = "1"
        md["resourceVersion"] = "99"

    api.before_patch = _peer_wrote_first

    provider, authority = _modelb_provider(api, rdb)
    try:
        shards = await provider.list_shards(_REGION)
    finally:
        await provider.aclose()

    assert captured["before"] == "1" and captured["before"] != "99"  # 确实漂移了

    assert api.conflicts == [("hub-b", "/metadata/resourceVersion")]
    assert len(api.patches) == 1  # 不靠重试猜结果:只发一次

    anns = api.gameservers["hub-b"]["metadata"]["annotations"]
    for key in A.DS_CREDENTIAL_ANNOTATION_KEYS:
        assert key not in anns, f"{key} 不该落地"

    assert len(shards) == 1
    assert shards[0].pod_name == "hub-b"
    assert shards[0].token_ready is False
    assert shards[0].token_gen == 0
    assert shards[0].token_exp_ms == 0

    rec, found = await hauth.RedisHubAuthRepo(rdb).get_auth("hub-b")
    assert found
    assert A.credential_fields_equal(rec.pending, authority.issued[0])
    assert rec.delivered_rv == ""  # 没确认送达 → 下轮重投


async def test_modelb_same_name_uid_rebuild_fails_closed(rdb) -> None:
    """同名 Pod 被重建(uid 变了)后,针对旧实例的投递必须被 uid test 拒掉。

    为什么重要:Agones 的 GameServer 名可以复用。只 CAS resourceVersion 挡不住
    "同名不同实例" —— 把上一实例的凭据 PATCH 到新实例上,新 DS 会拿着一份绑定旧
    instance_uid 的令牌,§9.22 要求的 exact 实例绑定当场失效。

    ★ 构造校验:断言重建后的 uid 与原 uid 不同。
    ★ 变异:agones_fleet.py `_patch_credential_annotation` 的 `ops` 里删掉
      `{"op": "test", "path": "/metadata/uid", ...}`。
    """
    api = FakeAgonesApi()
    api.gameservers["hub-c"] = _gs("hub-c", uid="uid-old", rv="1")
    captured: dict[str, str] = {}

    def _rebuilt(fake: FakeAgonesApi) -> None:
        md = fake.gameservers["hub-c"]["metadata"]
        captured["before"] = md["uid"]
        md["uid"] = "uid-new"

    api.before_patch = _rebuilt

    provider, _ = _modelb_provider(api, rdb)
    try:
        shards = await provider.list_shards(_REGION)
    finally:
        await provider.aclose()

    assert captured["before"] == "uid-old" and captured["before"] != "uid-new"

    assert api.conflicts == [("hub-c", "/metadata/uid")]
    anns = api.gameservers["hub-c"]["metadata"]["annotations"]
    assert A.DS_TOKEN_ANNOTATION_KEY not in anns
    assert shards[0].token_ready is False

    rec, found = await hauth.RedisHubAuthRepo(rdb).get_auth("hub-c")
    assert found and rec.delivered_rv == ""


async def test_modelb_reuses_matching_bundle_without_repatching(rdb) -> None:
    """annotation bundle 与 Redis pending **完全一致**时必须复用,不重签不重投。

    为什么重要:每轮对账都重签一次令牌,会让 gen 无限上涨、DS 侧刚拿到的令牌立刻过期,
    表现为"心跳时好时坏"。这条也是下一条(篡改必须触发重投)的对照组。

    ★ 变异:agones_fleet.py `_ensure_hub_credential` 的 2b 分支
      (`pending is not None and ... is None` 那段)删掉 —— 第二轮会重签,patch 数变 2。
    """
    api = FakeAgonesApi()
    api.gameservers["hub-d"] = _gs("hub-d", uid="uid-D", rv="1")
    provider, authority = _modelb_provider(api, rdb)
    try:
        first = await provider.list_shards(_REGION)
        second = await provider.list_shards(_REGION)
    finally:
        await provider.aclose()

    assert len(api.patches) == 1  # 第二轮没有再 PATCH
    assert len(authority.issued) == 1  # 也没有再签发
    assert first[0].token_gen == second[0].token_gen == 1
    assert second[0].token_ready is True


@pytest.mark.parametrize("key", list(A.DS_CREDENTIAL_ANNOTATION_KEYS))
async def test_modelb_any_tampered_annotation_forces_redelivery(rdb, key: str) -> None:
    """9 项 annotation 里**任何一项**被改动,都不得判成"已送达"。

    为什么重要:annotation 是 DS 实际读到的那份凭据。只要它与 Redis 权威记录有一个
    字段不同,DS 手上的令牌就不是权威认可的那份;此时若判成"已送达"就不再重投,
    这台 Hub 会永久停在"令牌看着有、心跳一直被拒"的状态。逐项参数化是因为漏掉任何
    一项比对都不会有别的信号。

    ★ 构造校验:断言篡改后的值确实**不等于**原值。
    ★ 变异:agones_fleet.py `_credential_bundle_matches` 里任意一项
      `if ann != ...: return "... mismatch"` 删掉,对应那一行参数立刻变红。
    """
    api = FakeAgonesApi()
    api.gameservers["hub-e"] = _gs("hub-e", uid="uid-E", rv="1")
    provider, authority = _modelb_provider(api, rdb)
    try:
        await provider.list_shards(_REGION)
        assert len(api.patches) == 1

        anns = api.gameservers["hub-e"]["metadata"]["annotations"]
        original = anns[key]
        anns[key] = original + "-tampered"
        assert anns[key] != original  # 构造出来的"篡改"确实不同

        shards = await provider.list_shards(_REGION)
    finally:
        await provider.aclose()

    assert len(authority.issued) == 2  # 重新签发
    assert len(api.patches) == 2  # 重新投递
    assert shards[0].token_gen == 2
    assert shards[0].token_ready is True
    assert api.gameservers["hub-e"]["metadata"]["annotations"][key] != original + "-tampered"


async def test_modelb_incomplete_dependencies_never_fall_back_to_legacy(rdb) -> None:
    """Model B 装配缺依赖时必须 fail-closed,**绝不**静默回退 legacy 令牌路径。

    为什么重要:静默降级等于把"Redis 是唯一授权权威"这条设计在一次配置错误里悄悄关掉,
    而所有日志都正常 —— 集群跑着跑着就没有了两阶段激活闸,谁也不知道从哪一刻开始的。

    ★ 变异:agones_fleet.py `_ensure_ds_token_or_credential` 里
      `if not self._model_b_active(): raise AgonesError(...)` 改成
      `return await self._ensure_ds_token(gs)`(legacy 签发器会被调用 → 本条变红)。
    """
    api = FakeAgonesApi()
    api.gameservers["hub-f"] = _gs("hub-f", uid="uid-F", rv="1")
    legacy_calls: list[str] = []

    async def _legacy_issuer(pod: str) -> tuple[str, int, int]:
        legacy_calls.append(pod)
        return "legacy-token", int(time.time() * 1000) + 600_000, 5

    provider = A.AgonesHubFleetProvider(_agones_cfg(), transport=api.transport())
    provider.set_ds_token_issuer(_legacy_issuer, _RENEW_BEFORE, True)
    authority = FakeHubCredAuthority()
    # verifier 缺失 —— 典型的"配置改了一半"。
    provider.set_hub_authority(
        hauth.RedisHubAuthRepo(rdb), authority.issue, None, _RENEW_BEFORE, _AUTH_TTL
    )
    try:
        shards = await provider.list_shards(_REGION)
    finally:
        await provider.aclose()

    assert legacy_calls == []  # legacy 路径一次都没被走
    assert api.patches == []
    assert len(shards) == 1
    assert shards[0].token_ready is False  # 分片仍在拓扑里,但不可分配
    assert shards[0].token_gen == 0


async def test_legacy_token_patch_conflict_rereads_and_reuses_peer_generation() -> None:
    """legacy 路径:merge-patch 的 rv CAS 冲突后必须**重读**并复用对方已写好的代际。

    为什么重要:没有 `metadata.resourceVersion` 这一行,后到的低代际 PATCH 会盖掉
    先到的高代际令牌,K8s 上的 gen 与 Redis 的 CurrentTokenGen 分叉 —— 心跳侧
    `gen_required` 一律判 stale,这台 Hub 永不可分配。冲突后重读还能天然收敛:
    对方已经写好当前代际时直接复用,不再 INCR 发号。

    ★ 构造校验:断言"对方写入前后的 rv"确实不同,且对方 gen(7)不等于本副本会发的 gen(99)。
    ★ 变异:agones_fleet.py `_ensure_ds_token` 的 merge-patch body 里删掉
      `"resourceVersion": cur.metadata.resource_version` —— 假 apiserver 不再拒,
      本副本的 gen=99 会盖掉对方的 gen=7,本条变红。
    """
    now_ms = int(time.time() * 1000)
    stale_exp = now_ms + 60_000  # 落在 10 分钟续期窗口内 → 必须重签
    fresh_exp = now_ms + 3_600_000

    api = FakeAgonesApi()
    api.gameservers["hub-cas"] = _gs(
        "hub-cas",
        uid="uid-cas",
        rv="111",
        annotations={
            A.DS_TOKEN_ANNOTATION_KEY: "tok-stale",
            A.DS_TOKEN_EXP_ANNOTATION_KEY: str(stale_exp),
            A.DS_TOKEN_GEN_ANNOTATION_KEY: "3",
        },
    )
    captured: dict[str, str] = {}

    def _peer_wrote_first(fake: FakeAgonesApi) -> None:
        md = fake.gameservers["hub-cas"]["metadata"]
        captured["before"] = md["resourceVersion"]
        md["annotations"].update(
            {
                A.DS_TOKEN_ANNOTATION_KEY: "tok-peer",
                A.DS_TOKEN_EXP_ANNOTATION_KEY: str(fresh_exp),
                A.DS_TOKEN_GEN_ANNOTATION_KEY: "7",
            }
        )
        md["resourceVersion"] = "222"

    api.before_patch = _peer_wrote_first

    issued: list[str] = []

    async def _issuer(pod: str) -> tuple[str, int, int]:
        issued.append(pod)
        return "tok-mine", fresh_exp, 99

    provider = A.AgonesHubFleetProvider(_agones_cfg(), transport=api.transport())
    provider.set_ds_token_issuer(_issuer, _RENEW_BEFORE, True)
    try:
        shards = await provider.list_shards(_REGION)
    finally:
        await provider.aclose()

    assert captured["before"] == "111" and captured["before"] != "222"

    assert api.conflicts == [("hub-cas", "/metadata/resourceVersion")]
    assert len(api.patches) == 1  # 冲突后重读发现已收敛,不再发第二次
    anns = api.gameservers["hub-cas"]["metadata"]["annotations"]
    assert anns[A.DS_TOKEN_ANNOTATION_KEY] == "tok-peer"  # 对方的写没被盖
    assert anns[A.DS_TOKEN_GEN_ANNOTATION_KEY] == "7"
    assert shards[0].token_gen == 7  # 复用对方代际,而不是本地的 99
    assert shards[0].token_ready is True
    assert issued == ["hub-cas"]  # 只签过一次(第一次尝试),冲突后没再发号


async def test_legacy_enforce_resigns_when_generation_annotation_missing() -> None:
    """enforce 下 exp 还很新但缺合法 `ds-token-gen` → 必须强制重签补齐代际。

    为什么重要:gen=0 的分片进对账后,心跳侧 `gen_required` 一律判 stale,这台 Hub
    永不可分配;更危险的是"legacy gen0 被当成有效"会把整个代际门关掉。

    ★ 构造校验:annotation 里的 exp 确实远在续期窗口之外(否则重签是因为过期,测错了原因)。
    ★ 变异:agones_fleet.py `_token_still_valid` 里
      `if self._ds_token_required and ann_gen == 0: return None` 删掉。
    """
    now_ms = int(time.time() * 1000)
    fresh_exp = now_ms + 3_600_000
    assert fresh_exp - now_ms > _RENEW_BEFORE.total_seconds() * 1000  # 不在续期窗口内

    api = FakeAgonesApi()
    api.gameservers["hub-gen0"] = _gs(
        "hub-gen0",
        uid="uid-gen0",
        rv="1",
        annotations={
            A.DS_TOKEN_ANNOTATION_KEY: "tok-legacy",
            A.DS_TOKEN_EXP_ANNOTATION_KEY: str(fresh_exp),
            # 刻意没有 ds-token-gen
        },
    )

    async def _issuer(pod: str) -> tuple[str, int, int]:
        return "tok-new", fresh_exp, 9

    provider = A.AgonesHubFleetProvider(_agones_cfg(), transport=api.transport())
    provider.set_ds_token_issuer(_issuer, _RENEW_BEFORE, True)
    try:
        shards = await provider.list_shards(_REGION)
    finally:
        await provider.aclose()

    assert len(api.patches) == 1
    assert api.patches[0][1] == "application/merge-patch+json"
    assert shards[0].token_gen == 9
    assert shards[0].token_ready is True
    assert api.gameservers["hub-gen0"]["metadata"]["annotations"][
        A.DS_TOKEN_GEN_ANNOTATION_KEY
    ] == "9"
