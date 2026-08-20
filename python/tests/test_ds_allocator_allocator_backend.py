"""ds_allocator **分配后端**回归测试 —— 覆盖两个可互换的 GameServerAllocator 实现:

    pandorapy/services/ds_allocator/agones_allocator.py   Agones/k8s(Linux 生产)
    pandorapy/services/ds_allocator/local_allocator.py    本机 exec UE DS(联调)

对应 Go 侧 `services/battle/ds_allocator/internal/data/{agones,local}_allocator.go`;
断言口径一律以那两份 Go 源码为准。

## 本文件盯死的四件事

  ① **resourceVersion / uid 条件 PATCH**(凭据投递)。JSON Patch 的两条 `test` op 是
     Model B 投递链上唯一的乐观并发闸。闸没了不会报错 —— 只会在多副本交错写同一个
     GameServer 时把别人刚写好的高代际凭据用自己的低代际盖掉,K8s annotation 与 Redis
     授权记录就此分叉,DS 拿着一份谁也不认的令牌。
     所以这里的假 apiserver **真的执行 test op**(不成立就 409 且一个 op 都不应用),
     "闸失效"才会表现成"本不该落地的写落地了",而不是靠断言请求体形状间接推断。
  ② **Stable / Canary 不互串**(§9 不变量 21)。selector 必须**同时**钉 Fleet 名与
     `release-track` 标签;而最终轨道要由回读到的 GameServer label + annotation
     一致证明,不能拿"分配意图"当"实际命中轨"。
  ③ **结果不确定时 fail-closed**(§9.22)。POST 传输失败 / 严格回读失败 / LIST 失败,
     一律报错并保留 `allocation_id` 这枚 fencing token;**绝不**冒充"没有可用 DS"
     (会让上游重发一次 POST → 同一局两台 Pod),也绝不冒充"分配成功"。
  ④ **无可用 GameServer 与 Fleet 没配是两回事**。前者是容量事实(`ErrDSNoAvailable`,
     该扩容),后者是配置错误(`ErrDSAllocationFailed`,扩容一辈子也没用)。
     INC-20260724-001 就是把后者混进前者口径,让运维照着扩容排查了一轮。

## 关于"空 release_track 回落 stable"

**这两个后端都没有这条回落,也不该有。** 逐条核对 Go:

  - `agones_allocator.go:322` / `:631`:`if !releasetrack.Valid(releaseTrack)` →
    直接 `ErrInvalidArg`,连 POST 都不发。`releasetrack.Valid("")` 是 false。
  - `local_allocator.go`:`Allocate` 对 `releaseTrack` **原样透传**,不校验也不改写。

"空 track 迁移成 stable" 是 **hub_allocator 持久化记录**那侧的 `sticky_release_track`
(给 pre-track 时代的老记录补默认值),方向与这里相反。把它抄到分配入口会让一个
本该被拒的非法/丢失轨道静默落到 stable Fleet —— 灰度回滚时"把 Canary 权重归零"
拦不住已经被静默改轨的分配。故本文件按 Go 的真实行为钉住"**拒绝**且零副作用"。

## 依赖策略

k8s apiserver 用 `httpx.MockTransport` 注入(`AgonesGameServerAllocator(cfg, transport=...)`
是产品代码明写的测试钩子),不 patch 任何内部状态。本机 DS 用注入的假进程,
不真的 exec UE。

★ 每条用例 docstring 的 `★ 变异:` 一行都真跑过(改坏 → 红 → 改回 → 绿)。
"""

from __future__ import annotations

import asyncio
import json
import sys
import urllib.parse
import uuid as _uuid
from typing import Any, Callable

import httpx
import pytest

from pandorapy import errcode, releasetrack
from pandorapy.services.ds_allocator import agones_allocator as A
from pandorapy.services.ds_allocator import conf as dconf
from pandorapy.services.ds_allocator import local_allocator as LA

_API = "https://apiserver.test"
_NS = "pandora"
_STABLE_FLEET = "pandora-battle"
_CANARY_FLEET = "pandora-battle-canary"

_MATCH_ID = 777_001
_MAP_ID = 4002
_GAME_MODE = "pve_coop"
_PLAYERS = [42, 7, 99]
_FACTIONS = {7: 1, 42: 1, 99: 2}


# ══ 假 apiserver ══════════════════════════════════════════════════════════════


def _json_resp(status: int, payload: Any) -> httpx.Response:
    return httpx.Response(
        status,
        content=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )


def _unescape_pointer(token: str) -> str:
    """JSON Pointer 反转义。**顺序与转义相反**(先 `~1` 再 `~0`),否则 `~01` 会被错拆成 `/`。"""
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


def _labels_match(want: dict[str, str], gs: dict[str, Any]) -> bool:
    labels = gs.get("metadata", {}).get("labels") or {}
    return all(labels.get(k) == v for k, v in want.items())


def _selector_matches(selector: str, gs: dict[str, Any]) -> bool:
    """只实现本文件用到的两种 selector 语法:`k=v,...` 与 `k in (a,b)`。"""
    labels = gs.get("metadata", {}).get("labels") or {}
    if " in (" in selector:
        key, _, rest = selector.partition(" in (")
        allowed = {s for s in rest.rstrip(")").split(",") if s}
        return labels.get(key.strip(), "") in allowed
    for term in selector.split(","):
        if term == "":
            continue
        key, _, want = term.partition("=")
        if labels.get(key) != want:
            return False
    return True


class FakeAgonesApi:
    """内存版 k8s/Agones apiserver(只实现本测试用到的动词),经 `httpx.MockTransport` 注入。

    ★ 它像真 apiserver 一样**执行** JSON Patch 的 `test` op:任一条件不成立就回 409
      且**不应用任何改动**。这是本文件"条件 PATCH"系列断言成立的前提 —— 只有服务端
      真的会拒,"生产代码把 test op 删了"才会表现成"数据被改了",而不是"请求体少了一行"。
    ★ 写落地后 `resourceVersion` 自增,对应真 apiserver 的语义。
    """

    def __init__(self) -> None:
        self.gameservers: dict[str, dict[str, Any]] = {}
        self.pods: dict[str, dict[str, Any]] = {}
        #: 每次 GSA POST 的请求体(断言 selector 分轨用)。
        self.allocations: list[dict[str, Any]] = []
        #: 每次 gameservers LIST 的 labelSelector 原文与分页游标。
        self.list_selectors: list[str] = []
        #: 每次 PATCH 的 (name, content-type, 已解码 body)。
        self.patches: list[tuple[str, str, Any]] = []
        #: 每次被 test op 拒掉的 (name, 条件路径)。
        self.conflicts: list[tuple[str, str]] = []
        self.gets: list[str] = []
        self.deletes: list[tuple[str, Any]] = []

        # ── 故障注入开关 ──
        #: GSA POST 抛传输错误(apiserver 不可达 / 超时):结果**未知**。
        self.allocate_transport_error = False
        #: GET gameservers/<name> 一律 404(严格回读失败)。
        self.get_gameserver_status: int | None = None
        #: gameservers LIST 抛传输错误。
        self.list_transport_error = False
        #: 第 N 页(0-based)LIST 返回的 HTTP 状态;其余页正常。
        self.list_page_status: dict[int, int] = {}
        #: LIST 每页返回条数(触发分页 continue)。
        self.list_page_limit: int | None = None
        #: PATCH 应用成功之后再抛传输错误(模拟"写落地了但响应没回来")。
        self.patch_transport_error_after_apply = False
        #: GSA POST 写 metadata 时故意丢掉这些 label(模拟别的写者覆盖 / Agones 版本差异)。
        # 模拟「命中的对象上写着另一条轨」:Agones 版本差异 / 别的写者覆盖 / Fleet 标签配歪。
        # 必须作用在 **GameServer 对象** 上,不能只删本次 allocate 补丁里的 label ——
        # 对象上还留着播种时的那条轨,漂移根本没被制造出来。
        self.drift_on_allocate: dict[str, str] = {}
        #: 一次性钩子:下一次 PATCH 到达前执行,用来模拟"别的副本抢先写了"。
        self.before_patch: Callable[[FakeAgonesApi], None] | None = None

        self._list_pages: list[list[dict[str, Any]]] = []

    # ── 播种 ──
    def add_gameserver(
        self,
        name: str,
        *,
        uid: str,
        fleet: str,
        track: str,
        rv: str = "1",
        state: str = "Ready",
        address: str = "10.0.0.7",
        port: int = 7777,
        with_pod: bool = True,
    ) -> None:
        self.gameservers[name] = {
            "metadata": {
                "name": name,
                "uid": uid,
                "resourceVersion": rv,
                "labels": {
                    A.FLEET_LABEL_KEY: fleet,
                    A.RELEASE_TRACK_METADATA_KEY: track,
                },
                "annotations": {A.RELEASE_TRACK_METADATA_KEY: track},
            },
            "status": {
                "state": state,
                "address": address,
                "ports": [{"name": "default", "port": port}],
            },
        }
        if with_pod:
            self.pods[name] = {
                "metadata": {
                    "name": name,
                    "uid": f"pod-{uid}",
                    "resourceVersion": rv,
                    "ownerReferences": [
                        {
                            "apiVersion": "agones.dev/v1",
                            "kind": "GameServer",
                            "name": name,
                            "uid": uid,
                            "controller": True,
                        }
                    ],
                }
            }

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self._handle)

    # ── 路由 ──
    def _handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        gsa = f"/apis/allocation.agones.dev/v1/namespaces/{_NS}/gameserverallocations"
        gs_list = f"/apis/agones.dev/v1/namespaces/{_NS}/gameservers"
        pod_prefix = f"/api/v1/namespaces/{_NS}/pods/"

        if request.method == "POST" and path == gsa:
            return self._allocate(request)
        if request.method == "GET" and path == gs_list:
            return self._list_gameservers(request)
        if path.startswith(gs_list + "/"):
            name = urllib.parse.unquote(path[len(gs_list) + 1 :])
            if request.method == "GET":
                self.gets.append(name)
                if self.get_gameserver_status is not None:
                    return _json_resp(self.get_gameserver_status, {"message": "injected"})
                obj = self.gameservers.get(name)
                if obj is None:
                    return _json_resp(404, {"message": "gameserver not found"})
                return _json_resp(200, obj)
            if request.method == "PATCH":
                return self._patch_gameserver(name, request)
            if request.method == "DELETE":
                self.deletes.append((name, json.loads(request.content or b"{}")))
                return _json_resp(200, {"status": "Success"})
        if request.method == "GET" and path.startswith(pod_prefix):
            name = urllib.parse.unquote(path[len(pod_prefix) :])
            obj = self.pods.get(name)
            if obj is None:
                return _json_resp(404, {"message": "pod not found"})
            return _json_resp(200, obj)

        return _json_resp(404, {"message": f"unhandled {request.method} {path}"})

    def _allocate(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        self.allocations.append(body)
        if self.allocate_transport_error:
            raise httpx.ConnectError("apiserver unreachable", request=request)
        spec = body["spec"]
        for selector in spec["selectors"]:
            for gs in self.gameservers.values():
                if gs["status"]["state"] != "Ready":
                    continue
                if not _labels_match(selector["matchLabels"], gs):
                    continue
                md = spec.get("metadata") or {}
                labels = dict(md.get("labels") or {})
                gs["metadata"].setdefault("labels", {}).update(labels)
                gs["metadata"].setdefault("annotations", {}).update(md.get("annotations") or {})
                # label 与 annotation 一起漂,才能把「轨自洽但 ≠ 本次意图」这一条
                # 单独隔离出来 —— 只漂 label 会连带触发 annotation 交叉核对那一条。
                for drifted, value in self.drift_on_allocate.items():
                    gs["metadata"]["labels"][drifted] = value
                    gs["metadata"]["annotations"][drifted] = value
                gs["metadata"]["resourceVersion"] = str(
                    int(gs["metadata"]["resourceVersion"]) + 1
                )
                gs["status"]["state"] = "Allocated"
                return _json_resp(
                    200,
                    {
                        "status": {
                            "state": "Allocated",
                            "gameServerName": gs["metadata"]["name"],
                            "address": gs["status"]["address"],
                            "ports": gs["status"]["ports"],
                        }
                    },
                )
        return _json_resp(200, {"status": {"state": "UnAllocated"}})

    def _list_gameservers(self, request: httpx.Request) -> httpx.Response:
        selector = request.url.params.get("labelSelector", "")
        self.list_selectors.append(selector)
        if self.list_transport_error:
            raise httpx.ConnectError("apiserver unreachable", request=request)
        items = [gs for gs in self.gameservers.values() if _selector_matches(selector, gs)]
        if self.list_page_limit is None:
            page = len(self.list_selectors) - 1
            if page in self.list_page_status:
                return _json_resp(self.list_page_status[page], {"message": "injected"})
            return _json_resp(200, {"items": items})
        # 分页:continue 游标就是下一页的起始下标(足够假 apiserver 用)。
        start = int(request.url.params.get("continue", "0") or 0)
        page = start // self.list_page_limit
        if page in self.list_page_status:
            return _json_resp(self.list_page_status[page], {"message": "injected"})
        chunk = items[start : start + self.list_page_limit]
        nxt = start + self.list_page_limit
        meta = {"continue": str(nxt)} if nxt < len(items) else {}
        return _json_resp(200, {"items": chunk, "metadata": meta})

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
        if ctype != "application/json-patch+json":
            return _json_resp(415, {"message": f"unsupported content-type {ctype!r}"})
        for op in body:
            if op.get("op") != "test":
                continue
            if _pointer_get(obj, op["path"]) != op.get("value"):
                self.conflicts.append((name, op["path"]))
                return _json_resp(409, {"message": "the object has been modified"})
        for op in body:
            if op.get("op") == "add":
                _pointer_add(obj, op["path"], op["value"])
        obj["metadata"]["resourceVersion"] = str(int(obj["metadata"]["resourceVersion"]) + 1)
        if self.patch_transport_error_after_apply:
            raise httpx.ReadTimeout("patch response lost", request=request)
        return _json_resp(200, obj)

    # ── 断言辅助 ──
    def rv(self, name: str) -> str:
        return self.gameservers[name]["metadata"]["resourceVersion"]

    def ann(self, name: str, key: str) -> str:
        return (self.gameservers[name]["metadata"].get("annotations") or {}).get(key, "")

    def label(self, name: str, key: str) -> str:
        return (self.gameservers[name]["metadata"].get("labels") or {}).get(key, "")


def _agones_cfg(
    *,
    fleet_name: str = _STABLE_FLEET,
    canary_fleet_name: str = _CANARY_FLEET,
    canary_percent: int = 50,
    canary_seed: str = "seed-1",
    advertise_host: str = "",
    map_fleets: list[dict[str, Any]] | None = None,
) -> dconf.AgonesConf:
    return dconf.AgonesConf(
        enabled=True,
        api_server=_API,
        namespace=_NS,
        fleet_name=fleet_name,
        canary_fleet_name=canary_fleet_name,
        canary_percent=canary_percent,
        canary_seed=canary_seed,
        advertise_host=advertise_host,
        token_path="-",  # 联调档:不带 Authorization
        map_fleets=[dconf.AgonesMapFleet(**mf) for mf in (map_fleets or [])],
    )


@pytest.fixture
def api() -> FakeAgonesApi:
    return FakeAgonesApi()


@pytest.fixture
async def allocator(api: FakeAgonesApi):
    """默认拓扑:stable / canary 各一台 Ready GameServer。"""
    api.add_gameserver("gs-stable", uid="uid-stable", fleet=_STABLE_FLEET, track="stable")
    api.add_gameserver(
        "gs-canary", uid="uid-canary", fleet=_CANARY_FLEET, track="canary", port=7778
    )
    alloc = A.AgonesGameServerAllocator(_agones_cfg(), transport=api.transport())
    try:
        yield alloc
    finally:
        await alloc.aclose()


def _selector_of(body: dict[str, Any], index: int = 0) -> dict[str, str]:
    return body["spec"]["selectors"][index]["matchLabels"]


async def _allocate_authoritative(allocator, allocation_id: str, track: str):
    return await allocator.allocate_authoritative(
        _MATCH_ID, allocation_id, list(_PLAYERS), dict(_FACTIONS), _MAP_ID, _GAME_MODE, track
    )


# ══ ① Stable / Canary 不互串 ═════════════════════════════════════════════════


async def test_canary_allocation_targets_only_the_canary_fleet_and_track(
    api: FakeAgonesApi, allocator
) -> None:
    """★ canary 请求的 selector 必须**同时**钉 canary Fleet 名与 `release-track=canary`。

    两个条件缺一个都不行:
      - 只钉 Fleet 名 → 一台被误打上 canary Fleet 标签的 stable 实例会被选中;
      - 只钉 track → 会命中另一个 Fleet 里恰好也叫 canary 的实例。
    §9 不变量 21 要求"同一玩家 / 同一对局固定 release track",而这条 selector 是那句话
    在分配入口的**唯一**机械落点。

    ★ 变异:把 `_allocate_once_with_metadata` 的 `selector_labels` 里
      `RELEASE_TRACK_METADATA_KEY: release_track` 删掉 → 本条红。
    """
    pod, addr, track = await allocator.allocate(
        _MATCH_ID, _MAP_ID, _GAME_MODE, releasetrack.CANARY
    )
    assert (pod, track) == ("gs-canary", releasetrack.CANARY)
    assert addr == "10.0.0.7:7778"

    labels = _selector_of(api.allocations[0])
    assert labels[A.FLEET_LABEL_KEY] == _CANARY_FLEET
    assert labels[A.RELEASE_TRACK_METADATA_KEY] == releasetrack.CANARY
    # 分配意图还必须写回被选中对象,供后续严格回读证明"实际命中轨"。
    assert api.label("gs-canary", A.RELEASE_TRACK_METADATA_KEY) == releasetrack.CANARY
    assert api.ann("gs-canary", A.RELEASE_TRACK_METADATA_KEY) == releasetrack.CANARY
    assert api.gameservers["gs-stable"]["status"]["state"] == "Ready", "stable 实例被误分配"


async def test_stable_allocation_never_reaches_the_canary_fleet(
    api: FakeAgonesApi, allocator
) -> None:
    """★ 反向:stable 请求**没有**任何回退到 canary 的路径。

    容量回退是单向的(canary 满了退 stable),因为 canary 是"少数人先吃螃蟹"。
    反向回退会把正式玩家静默塞进灰度实例 —— 回滚时"Canary 权重归零"完全拦不住。

    ★ 变异:在 `_allocate_with_metadata` 里给 stable 也 `tracks.append(CANARY)`
      → 本条红。
    """
    api.gameservers["gs-stable"]["status"]["state"] = "Allocated"  # stable 满
    with pytest.raises(errcode.PandoraError) as got:
        await allocator.allocate(_MATCH_ID, _MAP_ID, _GAME_MODE, releasetrack.STABLE)
    assert got.value.code == errcode.ErrDSNoAvailable
    assert len(api.allocations) == 1, "stable 请求发出了第二次 POST(回退到了 canary)"
    assert api.gameservers["gs-canary"]["status"]["state"] == "Ready"


async def test_canary_falls_back_to_stable_only_on_a_capacity_answer(
    api: FakeAgonesApi, allocator
) -> None:
    """★ canary → stable 回退**只在明确的容量事实**(`UnAllocated`)上发生。

    "明确"二字是全部重点:Agones 回 UnAllocated 意味着服务端确定没有空闲副本,
    这次 POST 确定没产生任何分配,再发一次是安全的。断言两次 POST 的 selector
    分别指向两条轨,且最终返回的 track 是**实际命中**的 stable 而不是原始意图 canary。

    ★ 变异:把 `_allocate_with_metadata` 的 `if exc.code != errcode.ErrDSNoAvailable
      or i == last: raise` 改成 `if i == last: raise` → 与下一条用例配合变红。
    """
    api.gameservers["gs-canary"]["status"]["state"] = "Allocated"  # canary 满
    pod, _, track = await allocator.allocate(
        _MATCH_ID, _MAP_ID, _GAME_MODE, releasetrack.CANARY
    )
    assert (pod, track) == ("gs-stable", releasetrack.STABLE)
    assert len(api.allocations) == 2
    assert _selector_of(api.allocations[0])[A.FLEET_LABEL_KEY] == _CANARY_FLEET
    assert _selector_of(api.allocations[1])[A.FLEET_LABEL_KEY] == _STABLE_FLEET


async def test_canary_never_falls_back_when_the_post_result_is_unknown(
    api: FakeAgonesApi, allocator
) -> None:
    """★ **结果未知就必须停手** —— apiserver 不可达时绝不再发第二次 POST。

    这是本文件最重要的一条。POST 超时不证明服务端没执行:很可能 GameServerAllocation
    已经生效、只是响应丢了。此时"顺手退 stable 再试一次"会得到**第二台**已分配 Pod,
    同一局两台 DS(§9 不变量 1 直接破),而 Redis 侧只认得其中一台,另一台成为
    永远无人认领的孤儿。

    ★ 变异:把 `_allocate_with_metadata` 的 `if exc.code != errcode.ErrDSNoAvailable
      or i == last: raise` 改成 `if i == last: raise` → 本条红(会发出 2 次 POST)。
    """
    api.allocate_transport_error = True
    with pytest.raises(errcode.PandoraError) as got:
        await allocator.allocate(_MATCH_ID, _MAP_ID, _GAME_MODE, releasetrack.CANARY)
    assert got.value.code == errcode.ErrDSAllocationFailed
    assert got.value.code != errcode.ErrDSNoAvailable, "把'结果未知'冒充成了'没有可用 DS'"
    assert len(api.allocations) == 1, "结果未知时发出了第二次 POST"


# ══ ② 无可用 GameServer / Fleet 没配:两种明确错误 ═══════════════════════════


async def test_no_available_gameserver_reports_the_capacity_error_code(
    api: FakeAgonesApi, allocator
) -> None:
    """★ 真没空闲副本 → `ErrDSNoAvailable`,且错误消息带上 Agones 的 `state`。

    `UnAllocated`(该扩容)与 `Contention`(并发抢占,重试即可)处置截然不同,
    错误消息里没有 state 的话运维只能看到一句"分配失败"。

    ★ 变异:把 `_allocate_once_with_metadata` 里 `if state != AGONES_STATE_ALLOCATED`
      的错误码改成 `ErrDSAllocationFailed` → 本条红。
    """
    for gs in api.gameservers.values():
        gs["status"]["state"] = "Allocated"
    with pytest.raises(errcode.PandoraError) as got:
        await allocator.allocate(_MATCH_ID, _MAP_ID, _GAME_MODE, releasetrack.STABLE)
    assert got.value.code == errcode.ErrDSNoAvailable
    assert "UnAllocated" in str(got.value)


async def test_missing_fleet_config_is_reported_as_a_config_error_not_capacity(
    api: FakeAgonesApi,
) -> None:
    """★ Fleet 名没配 → `ErrDSAllocationFailed`(配置错误),**不是** `ErrDSNoAvailable`,
    而且一次 POST 都不发。

    INC-20260724-001 的原形:未配 canary Fleet 时旧逻辑报容量码,把配置错误混进
    "无空闲副本"的口径,运维照着扩容排查了一轮。扩容一辈子也修不好一个空字符串。

    ★ 变异:把 `_allocate_once_with_metadata` 里 `if general_fleet == ""` 分支的
      错误码改回 `errcode.ErrDSNoAvailable` → 本条红。
    """
    api.add_gameserver("gs-stable", uid="uid-stable", fleet=_STABLE_FLEET, track="stable")
    alloc = A.AgonesGameServerAllocator(
        _agones_cfg(canary_fleet_name="", canary_percent=0, canary_seed=""),
        transport=api.transport(),
    )
    try:
        with pytest.raises(errcode.PandoraError) as got:
            await alloc.allocate(_MATCH_ID, _MAP_ID, _GAME_MODE, releasetrack.CANARY)
    finally:
        await alloc.aclose()
    assert got.value.code == errcode.ErrDSAllocationFailed
    assert got.value.code != errcode.ErrDSNoAvailable
    assert api.allocations == [], "Fleet 没配却仍然发了 POST"


@pytest.mark.parametrize("track", ["", "STABLE", "prod", "canary-2"])
async def test_an_invalid_or_empty_release_track_is_refused_before_any_post(
    api: FakeAgonesApi, allocator, track: str
) -> None:
    """★ 非法 / 空 `release_track` → `ErrInvalidArg`,**连 POST 都不发**。

    这里刻意钉住"**不**回落 stable"(见模块头注释):`releasetrack.valid` 是 fail-closed,
    空串与未知轨道一律 false。静默补成 stable 的话,一个上游 bug 造成的空 track 会
    表现为"分配成功且一切正常",直到有人问"为什么灰度比例对不上"。

    大小写也不放过(`"STABLE"`):轨道名是跨语言 / 跨仓库的裸字符串常量,
    Agones Fleet 标签与 Redis 已落盘记录全是小写字面量。

    ★ 变异:在 `allocate` 开头加 `release_track = release_track or releasetrack.STABLE`
      → 空串那条参数红。
    """
    with pytest.raises(errcode.PandoraError) as got:
        await allocator.allocate(_MATCH_ID, _MAP_ID, _GAME_MODE, track)
    assert got.value.code == errcode.ErrInvalidArg
    assert api.allocations == [], "非法 track 却仍然发了 POST"


# ══ ③ 权威分配:严格回读 + fencing token 不丢 ════════════════════════════════


async def test_authoritative_allocation_binds_the_exact_instance(
    api: FakeAgonesApi, allocator
) -> None:
    """★ 权威分配必须回读出**完整 exact 身份**:uid + pod_uid + resourceVersion + 实际轨。

    这三件身份是后续重连签票、凭据投递条件 PATCH、孤儿回收 fencing 的全部依据;
    少任何一件,后面每一条链路都只能"尽力而为"。

    ★ 变异:把 `allocate_authoritative` 严格回读里的 `or gs.metadata.uid == ""` 删掉
      → 本条红(下面 `instance_uid` 断言会拿到空串)。
    """
    allocation_id = str(_uuid.uuid4())
    out = await _allocate_authoritative(allocator, allocation_id, releasetrack.STABLE)
    assert out.pod_name == "gs-stable"
    assert out.instance_uid == "uid-stable"
    assert out.pod_uid == "pod-uid-stable"
    assert out.resource_version == api.rv("gs-stable")
    assert out.allocation_id == allocation_id
    assert out.release_track == releasetrack.STABLE
    assert out.annotations_present is True
    # 花名册 / 阵营是同一份对局定义的两半,必须同时落到 annotation 上。
    assert api.ann("gs-stable", A.BATTLE_ROSTER_ANNOTATION_KEY) == "7,42,99"
    assert api.ann("gs-stable", A.BATTLE_COMBAT_FACTIONS_ANNOTATION_KEY) == "7=1,42=1,99=2"
    assert api.ann("gs-stable", A.BATTLE_ALLOCATION_METADATA_KEY) == allocation_id


async def test_authoritative_allocation_refuses_a_track_that_drifted_on_readback(
    api: FakeAgonesApi, allocator
) -> None:
    """★ **分配意图 ≠ 实际命中轨**:回读到的 `release-track` 与本次意图不符即判分配失败。

    Agones 版本差异、别的写者覆盖、Fleet 标签配歪,都会让"我以为分到了 canary"与
    "对象上写着 stable"分叉。放行的话,这一局的所有后续判定(灰度粘滞、回滚范围)
    都建立在一个假事实上。

    ★ 变异:把 `allocate_authoritative` 里
      `or actual_release_track != selected_track` 删掉 → 本条红。
    """
    # 对象上 label 与 annotation 都是 stable,自身自洽、也是合法轨 —— 唯一被打断的
    # 就是「意图 canary」与「实际 stable」的一致证明,于是本条只压这一个判定。
    api.drift_on_allocate = {A.RELEASE_TRACK_METADATA_KEY: releasetrack.STABLE}
    allocation_id = str(_uuid.uuid4())
    with pytest.raises(A.BattleAllocationError) as got:
        await _allocate_authoritative(allocator, allocation_id, releasetrack.CANARY)
    assert got.value.code == errcode.ErrDSAllocationFailed
    assert got.value.allocation.allocation_id == allocation_id


async def test_a_failed_strict_readback_still_returns_the_allocation_id_fencing_token(
    api: FakeAgonesApi, allocator
) -> None:
    """★ 严格回读失败时,`allocation_id` 这枚 fencing token **必须**交还调用方。

    POST 已经成功(有一台 Pod 被分配出去了),只是我们读不回来。此时把
    `allocation_id` 丢掉,调用方就只能删掉 claim 重新分配 —— 于是第二台 Pod 被分出来,
    第一台永远无人认领。`allocation_id` 是把那台 Pod 找回来 / 回收掉的唯一线索。

    ★ 变异:把 `allocate_authoritative` 里严格 GET 失败分支的 `allocation=partial`
      去掉 → 本条红。
    """
    api.get_gameserver_status = 500
    allocation_id = str(_uuid.uuid4())
    with pytest.raises(A.BattleAllocationError) as got:
        await _allocate_authoritative(allocator, allocation_id, releasetrack.STABLE)
    assert got.value.code == errcode.ErrDSAllocationFailed
    assert got.value.allocation is not None
    assert got.value.allocation.allocation_id == allocation_id
    assert got.value.allocation.pod_name == "gs-stable", "已知的部分结果也应保留"


async def test_authoritative_allocation_requires_combat_factions(allocator) -> None:
    """★ 缺阵营与缺花名册同级 —— 都是"对局定义不完整",一律拒。

    阵营曾是可选的,于是"名单齐了但阵营缺失"能分配成功,DS 拿到后退化成每人一个
    独立阵营的混战:队友互相能打,而且**看起来一切正常**(能进图、能打、能结算)。
    错误被玩成了功能,这类 bug 最难被发现。

    ★ 变异:把 `allocate_authoritative` 里 `if not combat_faction_by_player:` 整段删掉
      → 本条红。
    """
    with pytest.raises(A.BattleAllocationError) as got:
        await allocator.allocate_authoritative(
            _MATCH_ID,
            str(_uuid.uuid4()),
            list(_PLAYERS),
            {},
            _MAP_ID,
            _GAME_MODE,
            releasetrack.STABLE,
        )
    assert got.value.code == errcode.ErrInvalidArg


@pytest.mark.parametrize("allocation_id", ["", "not-a-uuid", "550E8400-E29B-41D4-A716-446655440000"])
async def test_authoritative_allocation_requires_a_canonical_uuid_v4(
    api: FakeAgonesApi, allocator, allocation_id: str
) -> None:
    """★ `allocation_id` 必须是 **canonical 小写 UUIDv4**,大写形式同样拒。

    它要同时当 k8s label value(会被 `sanitize_label_value` 规整)和 Redis 里的
    精确比对值。允许非 canonical 形式,就会出现"label 上是规整过的样子、记录里是原样"
    这种两边对不上的分配,回收链再也找不到它。

    ★ 变异:把 `_parse_canonical_uuid_v4` 里 `if s != str(parsed): return None`
      这类 canonical 复核删掉 → 大写那条参数红。
    """
    with pytest.raises(A.BattleAllocationError) as got:
        await _allocate_authoritative(allocator, allocation_id, releasetrack.STABLE)
    assert got.value.code == errcode.ErrInvalidArg
    assert api.allocations == [], "非法 allocation_id 却仍然发了 POST"


# ══ ④ 凭据投递:resourceVersion / uid 条件 PATCH ═════════════════════════════


def _cred_annotations(gen: int = 1) -> dict[str, str]:
    return {
        A.DS_TOKEN_ANNOTATION_KEY: f"token-gen-{gen}",
        "pandora.dev/ds-cred-gen": str(gen),
    }


async def test_credential_patch_is_rejected_when_the_resource_version_moved(
    api: FakeAgonesApi, allocator
) -> None:
    """★ **resourceVersion 变化 → 条件 PATCH 被拒**,且一个 annotation 都不许落地。

    模拟"另一个副本抢先写了同一个 GameServer":PATCH 到达前对象被改动、rv 抬高。
    没有这道 `test` op 的话,本次(可能是旧代际的)凭据会把别人刚写好的新代际盖掉,
    K8s annotation 与 Redis 授权记录就此分叉,DS 手里的令牌谁也不认。

    断言分三层:
      1. 抛 `ErrDSAllocationFailed`(不是静默成功);
      2. 假 apiserver 记录到的冲突路径就是 `/metadata/resourceVersion`;
      3. 目标 annotation **没有**出现在对象上 —— 闸真的拦住了写,而不是"请求体里有那行"。

    ★ 变异:把 `deliver_credential` 里
      `{"op": "test", "path": "/metadata/resourceVersion", ...}` 这条 op 删掉 → 本条红。
    """
    allocation = await _allocate_authoritative(allocator, str(_uuid.uuid4()), releasetrack.STABLE)

    def bump(fake: FakeAgonesApi) -> None:
        md = fake.gameservers["gs-stable"]["metadata"]
        md["resourceVersion"] = str(int(md["resourceVersion"]) + 1)
        md.setdefault("annotations", {})["pandora.dev/other-writer"] = "1"

    api.before_patch = bump
    with pytest.raises(errcode.PandoraError) as got:
        await allocator.deliver_credential(allocation, _cred_annotations())
    assert got.value.code == errcode.ErrDSAllocationFailed
    assert ("gs-stable", "/metadata/resourceVersion") in api.conflicts
    assert api.ann("gs-stable", A.DS_TOKEN_ANNOTATION_KEY) == "", "条件 PATCH 没拦住写"


async def test_credential_patch_succeeds_after_refreshing_the_resource_version(
    api: FakeAgonesApi, allocator
) -> None:
    """★ 拿到最新 rv 重试后必须成功,并返回**写落地后**的新 rv。

    与上一条成对:光证明"会被拒"不够,还要证明这是一道**可重试**的乐观锁而不是死路。
    返回值必须来自确认 GET(写落地后的 rv),把它当下一轮轮换的条件 —— 返回请求里那个
    旧 rv 的话,下一次 PATCH 必然 409,凭据轮换永远推进不下去。

    ★ 变异:把 `deliver_credential` 结尾的 `return confirmed.metadata.resource_version`
      改成 `return allocation.resource_version` → 本条红。
    """
    allocation = await _allocate_authoritative(allocator, str(_uuid.uuid4()), releasetrack.STABLE)
    api.before_patch = lambda fake: fake.gameservers["gs-stable"]["metadata"].__setitem__(
        "resourceVersion", str(int(fake.rv("gs-stable")) + 1)
    )
    with pytest.raises(errcode.PandoraError):
        await allocator.deliver_credential(allocation, _cred_annotations())

    allocation.resource_version = api.rv("gs-stable")  # 重读后重试
    new_rv = await allocator.deliver_credential(allocation, _cred_annotations())
    assert api.ann("gs-stable", A.DS_TOKEN_ANNOTATION_KEY) == "token-gen-1"
    assert new_rv == api.rv("gs-stable")
    assert new_rv != allocation.resource_version, "写落地后 rv 必须推进"


async def test_credential_patch_is_rejected_when_the_instance_uid_moved(
    api: FakeAgonesApi, allocator
) -> None:
    """★ 第二条 `test` op:`uid` 不符即拒 —— 同名 GameServer 换代不得继承凭据。

    rv 只能证明"对象没被改过";换代(Pod 删了重建,名字一样、UID 全新)会让 rv 从头计,
    完全可能撞上一个"看起来对得上"的值。uid 是这里唯一钉得住实例身份的东西。

    ★ 变异:把 `deliver_credential` 里 `{"op": "test", "path": "/metadata/uid", ...}`
      删掉 → 本条红。
    """
    allocation = await _allocate_authoritative(allocator, str(_uuid.uuid4()), releasetrack.STABLE)
    allocation.instance_uid = "uid-recreated"  # 手里拿的是旧实例身份

    with pytest.raises(errcode.PandoraError) as got:
        await allocator.deliver_credential(allocation, _cred_annotations())
    assert got.value.code == errcode.ErrDSAllocationFailed
    assert ("gs-stable", "/metadata/uid") in api.conflicts
    assert api.ann("gs-stable", A.DS_TOKEN_ANNOTATION_KEY) == ""


async def test_credential_delivery_confirms_by_readback_even_when_the_patch_response_is_lost(
    api: FakeAgonesApi, allocator
) -> None:
    """★ PATCH **超时不等于没生效** —— 必须再做一次严格确认 GET,确认到就算成功。

    这是产品代码把传输错误装进 `KubeResponse.error` 而不是抛异常的全部理由:
    抛异常会让调用点在 `await` 处直接跳走,那段 fail-closed 的确认逻辑整块失效,
    于是一次网络抖动被当成"投递失败",下轮重签一张新令牌把已经送达的旧令牌覆盖掉
    (DS 手里的令牌从此对不上权威记录)。

    ★ 变异:把 `deliver_credential` 里的确认 GET 段落改成"patch.error 非空就直接抛"
      → 本条红。
    """
    allocation = await _allocate_authoritative(allocator, str(_uuid.uuid4()), releasetrack.STABLE)
    api.patch_transport_error_after_apply = True

    new_rv = await allocator.deliver_credential(allocation, _cred_annotations(gen=3))
    assert api.ann("gs-stable", A.DS_TOKEN_ANNOTATION_KEY) == "token-gen-3"
    assert new_rv == api.rv("gs-stable")


@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(lambda a: setattr(a, "resource_version", ""), id="no-rv"),
        pytest.param(lambda a: setattr(a, "instance_uid", ""), id="no-uid"),
        pytest.param(lambda a: setattr(a, "pod_name", ""), id="no-pod"),
    ],
)
async def test_credential_delivery_refuses_an_incomplete_allocation(
    api: FakeAgonesApi, allocator, mutate
) -> None:
    """★ 缺 rv / uid / pod 名时直接 `ErrInvalidArg`,**不发 PATCH**。

    缺 rv 的 PATCH 就是无条件写。这条守卫是"条件 PATCH"在入口处的兜底:
    不挡的话,一条 `resource_version=""` 的分配记录会静默退化成无条件覆盖。

    ★ 变异:把 `deliver_credential` 开头 `or allocation.resource_version == ""` 删掉
      → `no-rv` 那条参数红。
    """
    allocation = await _allocate_authoritative(allocator, str(_uuid.uuid4()), releasetrack.STABLE)
    before = len(api.patches)
    mutate(allocation)
    with pytest.raises(errcode.PandoraError) as got:
        await allocator.deliver_credential(allocation, _cred_annotations())
    assert got.value.code == errcode.ErrInvalidArg
    assert len(api.patches) == before, "入参不完整却仍然发了 PATCH"


async def test_credential_delivery_refuses_empty_annotation_values(allocator) -> None:
    """★ 空 annotation 值一律拒 —— 空令牌落到 GameServer 上等于"投递成功但 DS 没令牌"。

    DS 侧读到空串与读不到 annotation 是同一种表现,于是这一局会以"心跳全被判未授权"
    收场,而 allocator 这边显示投递成功。

    ★ 变异:把 `deliver_credential` 里 `if k == "" or v == "": raise` 删掉 → 本条红。
    """
    allocation = await _allocate_authoritative(allocator, str(_uuid.uuid4()), releasetrack.STABLE)
    with pytest.raises(errcode.PandoraError) as got:
        await allocator.deliver_credential(allocation, {A.DS_TOKEN_ANNOTATION_KEY: ""})
    assert got.value.code == errcode.ErrInvalidArg


# ══ ⑤ 不确定分配的只读对账:查不到 ≠ 查不了 ═══════════════════════════════════


async def test_resolve_returns_absent_only_when_the_list_really_came_back_empty(
    api: FakeAgonesApi, allocator
) -> None:
    """★ 零对象 = 权威缺席,是"可以安全地当作没分配过"的**唯一**良性结局。

    先钉住正样本(真的空 → `(None, False)`),下一条再钉住"查不了"必须报错 ——
    两条合起来才证明这两种情况没被合并。

    ★ 变异:把 `resolve_allocation_by_id` 的 `if len(items) == 0: return None, False`
      改成 `return None, True` → 本条红。
    """
    got, found = await allocator.resolve_allocation_by_id(
        _MATCH_ID, str(_uuid.uuid4()), list(_PLAYERS), dict(_FACTIONS), _MAP_ID, _GAME_MODE
    )
    assert (got, found) == (None, False)


async def test_resolve_raises_instead_of_reporting_absent_when_the_list_fails(
    api: FakeAgonesApi, allocator
) -> None:
    """★ LIST 失败必须**抛错**,绝不返回 `(None, False)`。

    把"查不到"与"查不了"混成一件事,就等于用一次 apiserver 抖动换来"可以安全地当作
    没分配过"的结论(§9.22 明令禁止)。上游据此删 claim 重新分配 → 同一局第二台 Pod。

    ★ 变异:把 `resolve_allocation_by_id` 里 `if resp.error is not None: ... raise`
      改成 `return None, False` → 本条红。
    """
    api.list_transport_error = True
    with pytest.raises(errcode.PandoraError) as got:
        await allocator.resolve_allocation_by_id(
            _MATCH_ID, str(_uuid.uuid4()), list(_PLAYERS), dict(_FACTIONS), _MAP_ID, _GAME_MODE
        )
    assert got.value.code == errcode.ErrDSAllocationFailed


async def test_resolve_refuses_to_guess_when_one_allocation_id_maps_to_two_gameservers(
    api: FakeAgonesApi, allocator
) -> None:
    """★ 一个 `allocation_id` 对上两台 GameServer → 报错等人工介入,**不猜、不自动删**。

    "每局一台"的不变量已经破了。此时自动挑一台留、删另一台,可能删掉的正是玩家正在
    打的那台。这是少数几个"宁可停在这里也不要自动处置"的地方。

    ★ 变异:把 `resolve_allocation_by_id` 的 `if len(items) > 1: raise` 改成
      `items = items[:1]` → 本条红。
    """
    allocation_id = str(_uuid.uuid4())
    label = A.sanitize_label_value(allocation_id)
    for name in ("gs-stable", "gs-canary"):
        api.gameservers[name]["metadata"]["labels"][A.BATTLE_ALLOCATION_METADATA_KEY] = label

    with pytest.raises(errcode.PandoraError) as got:
        await allocator.resolve_allocation_by_id(
            _MATCH_ID, allocation_id, list(_PLAYERS), dict(_FACTIONS), _MAP_ID, _GAME_MODE
        )
    assert got.value.code == errcode.ErrDSAllocationFailed
    assert "ambiguous" in str(got.value)
    assert api.deletes == [], "歧义状态下动了删除"


async def test_resolve_finds_the_exact_instance_for_an_uncertain_post(
    api: FakeAgonesApi, allocator
) -> None:
    """★ 正样本:POST 结果未知但确实生效时,对账必须还原出完整 exact 身份。

    这条是"结果未知不许当没分配"的下半场:上游保留 `allocation_id` 之后,必须真的
    能靠它把那台 Pod 找回来接管,否则保留 token 也只是个摆设。

    ★ 变异:把 `resolve_allocation_by_id` 严格比对里的
      `or gs.metadata.ann(BATTLE_ROSTER_ANNOTATION_KEY) != roster` 删掉 → 本条不红
      (它是放宽方向);改成比对一个不存在的 key 才会红 —— 故此条只作正样本锚点,
      收紧方向的断言由上面的 ambiguous / list-failed 两条负责。
    """
    allocation_id = str(_uuid.uuid4())
    await _allocate_authoritative(allocator, allocation_id, releasetrack.STABLE)

    got, found = await allocator.resolve_allocation_by_id(
        _MATCH_ID, allocation_id, list(_PLAYERS), dict(_FACTIONS), _MAP_ID, _GAME_MODE
    )
    assert found is True
    assert got is not None
    assert got.pod_name == "gs-stable"
    assert got.instance_uid == "uid-stable"
    assert got.pod_uid == "pod-uid-stable"
    assert got.release_track == releasetrack.STABLE


# ══ ⑥ 孤儿清扫的清单:部分清单比没有清单更危险 ═══════════════════════════════


async def test_listing_allocated_gameservers_fails_the_whole_round_on_a_page_error(
    api: FakeAgonesApi, allocator
) -> None:
    """★ 分页 LIST 中途失败 → 整轮失败,**绝不返回半份清单**。

    这份清单的唯一用途是"哪些 Allocated GameServer 在 Redis 里找不到主 → 判孤儿回收"。
    半份清单意味着后半页那些**仍被引用**的 GameServer 集体消失,于是被当成孤儿删掉 ——
    正在打的对局被直接删库。

    ★ 变异:把 `list_allocated_game_servers` 里第二页的
      `raise AgonesError(...)` 改成 `return out` → 本条红。
    """
    for i in range(3):
        api.add_gameserver(f"gs-x{i}", uid=f"uid-x{i}", fleet=_STABLE_FLEET, track="stable")
        api.gameservers[f"gs-x{i}"]["status"]["state"] = "Allocated"
    api.list_page_limit = 2
    api.list_page_status = {1: 500}  # 第二页炸

    with pytest.raises(A.AgonesError) as got:
        await allocator.list_allocated_game_servers()
    assert "page 1" in str(got.value)


async def test_listing_allocated_gameservers_walks_every_page(
    api: FakeAgonesApi, allocator
) -> None:
    """★ 正样本:必须一直取到 `metadata.continue` 为空,只取首页就是"部分清单"的另一种形态。

    Fleet 一大,单页装不下;只取首页会让后半段 GameServer 从清单里消失,后果同上一条。

    ★ 变异:把 `list_allocated_game_servers` 的 `if continue_token == "": return out`
      改成无条件 `return out` → 本条红。
    """
    api.gameservers.clear()
    for i in range(5):
        api.add_gameserver(f"gs-y{i}", uid=f"uid-y{i}", fleet=_STABLE_FLEET, track="stable")
        api.gameservers[f"gs-y{i}"]["status"]["state"] = "Allocated"
    api.list_page_limit = 2

    out = await allocator.list_allocated_game_servers()
    assert sorted(item.name for item in out) == [f"gs-y{i}" for i in range(5)]
    assert all(item.uid != "" and item.fleet == _STABLE_FLEET for item in out)


# ══ ⑦ 本机分配后端(mode=local)══════════════════════════════════════════════


class FakeProc:
    """假 DS 进程:`wait()` 挂到 `kill()` 为止,不真的 exec UE。"""

    def __init__(self) -> None:
        self.killed = False
        self._done = asyncio.Event()

    async def kill(self) -> None:
        self.killed = True
        self._done.set()

    async def wait(self) -> None:
        await self._done.wait()


def _local_cfg(**kw: Any) -> dconf.LocalDSConf:
    base: dict[str, Any] = {
        "enabled": True,
        "launcher": dconf.LAUNCHER_PACKAGED,
        # 用当前解释器当"可执行文件":构造函数只检查存在性,不会真的运行它
        # (`start_proc` 已被测试替换)。
        "executable_path": sys.executable,
        "loader_map": "/Game/Maps/Loader",
        "advertise_host": "127.0.0.1",
        "port_base": 47000,
        "port_range": 4,
    }
    base.update(kw)
    return dconf.LocalDSConf(**base)


class LocalHarness:
    """本机 allocator + 记账用的假启动器。"""

    def __init__(self, cfg: dconf.LocalDSConf) -> None:
        self.alloc = LA.LocalGameServerAllocator(cfg)
        self.launched: list[tuple[str, int, int, int, str, str, str]] = []
        self.procs: list[FakeProc] = []
        self.alloc.port_probe = None  # 不探真端口(单测默认放行)
        self.alloc.start_proc = self._start

    async def _start(
        self,
        pod_name: str,
        port: int,
        match_id: int,
        map_id: int,
        map_url: str,
        game_mode: str,
        token: str,
    ) -> LA.DSProcess:
        self.launched.append((pod_name, port, match_id, map_id, map_url, game_mode, token))
        proc = FakeProc()
        self.procs.append(proc)
        return proc

    async def aclose(self) -> None:
        await self.alloc.close()
        await asyncio.sleep(0)  # 让 reaper 协程收尾,避免遗留 pending task


@pytest.fixture
async def local():
    h = LocalHarness(_local_cfg())
    try:
        yield h
    finally:
        await h.aclose()


async def test_local_allocate_passes_the_release_track_through_untouched(local) -> None:
    """★ 本机后端对 `release_track` **原样透传**,包括空串 —— 不校验、更不补 stable。

    本机没有 Stable/Canary 双 Fleet,轨道由上游决定并由 biz 侧复核(Go 的
    `local_allocator.go` 注释明写)。这里若"顺手补个 stable",就会把上游的一个空值
    bug 变成"看起来一切正常"的静默错轨 —— 而 biz 侧那道复核再也发现不了。

    ★ 变异:在 `local_allocator.allocate` 里加
      `release_track = release_track or releasetrack.STABLE` → 本条红。
    """
    for track in (releasetrack.CANARY, ""):
        pod, addr, got = await local.alloc.allocate(
            _MATCH_ID + hash(track) % 100, _MAP_ID, _GAME_MODE, track
        )
        assert got == track, "release_track 被改写了"
        assert pod.startswith("pandora-battle-local-")
        assert addr.startswith("127.0.0.1:")


async def test_local_allocate_is_idempotent_for_the_same_match(local) -> None:
    """★ 同一 match 重复分配必须**幂等**:返回原地址,不再拉第二个进程。

    AllocateBattle 的响应可能丢失,上游会重试。每次重试都 exec 一个新 DS 的话,同一局
    会有两个进程抢同一份对局定义,而台账只记得后一个 —— 前一个成了占着端口的幽灵。

    ★ 变异:把 `allocate` 里 `existing = self._procs.get(pod_name)` 那段幂等短路删掉
      → 本条红。
    """
    first = await local.alloc.allocate(_MATCH_ID, _MAP_ID, _GAME_MODE, releasetrack.STABLE)
    second = await local.alloc.allocate(_MATCH_ID, _MAP_ID, _GAME_MODE, releasetrack.STABLE)
    assert first == second
    assert len(local.launched) == 1


async def test_local_allocate_reports_no_available_when_the_port_pool_is_exhausted(
    local,
) -> None:
    """★ 端口池耗尽 → `ErrDSNoAvailable`(容量事实),错误消息带上区间。

    与 Agones 侧"无空闲 GameServer"同一口径:这是容量问题,上游据此判定性失败并给
    玩家一个明确答复,而不是无限等待。

    ★ 变异:把 `_pick_port_locked` 返回 None 时的错误码改成 `ErrDSAllocationFailed`
      → 本条红。
    """
    for i in range(4):  # port_range=4
        await local.alloc.allocate(_MATCH_ID + i, _MAP_ID, _GAME_MODE, releasetrack.STABLE)
    with pytest.raises(errcode.PandoraError) as got:
        await local.alloc.allocate(_MATCH_ID + 99, _MAP_ID, _GAME_MODE, releasetrack.STABLE)
    assert got.value.code == errcode.ErrDSNoAvailable
    assert "47000" in str(got.value) and "47004" in str(got.value)


async def test_local_allocate_skips_ports_the_probe_says_are_taken(local) -> None:
    """★ `port_probe` 判定被占的端口必须跳过 —— 幽灵 DS / 外部程序占用都算。

    UE DS 在 `-port=X` 被占时会**静默 fallback 到 X+1**,于是 allocator 记录并返回的
    端口(X)与 DS 实际监听端口(X+1)不一致:新对局客户端拿新 ticket 连到 X 上的旧 DS,
    被 PreLogin 拒 —— 表现为"匹配成功但进不去",且两边日志都正常。

    ★ 变异:把 `_pick_port_locked` 里 `if self.port_probe is not None and not self.port_probe(p): continue`
      删掉 → 本条红。
    """
    local.alloc.port_probe = lambda p: p >= 47002
    _, addr, _ = await local.alloc.allocate(_MATCH_ID, _MAP_ID, _GAME_MODE, releasetrack.STABLE)
    assert addr == "127.0.0.1:47002"


async def test_local_allocate_fails_closed_when_the_map_cannot_be_resolved() -> None:
    """★ 关卡解析失败 → `ErrInvalidArg` 且**一个进程都不拉起**,绝不回退兜底图。

    2026-08-04 实测事故的直接成因:起错图的 DS 会被 DS 侧关卡门判 Mismatch 后自杀,
    分配卡在 warming 直到 ready_wait 超时,玩家侧只看到"一直排队中"。让它当场失败,
    matchmaker 立刻拿到一条写明原因的错误。

    ★ 变异:在 `_resolve_startup_map` 里给 `self._map_url_resolver is None` 分支
      返回一个默认图 → 本条红。
    """
    h = LocalHarness(_local_cfg(loader_map=""))  # 无 loader_map 且未注入 resolver
    try:
        with pytest.raises(errcode.PandoraError) as got:
            await h.alloc.allocate(_MATCH_ID, _MAP_ID, _GAME_MODE, releasetrack.STABLE)
        assert got.value.code == errcode.ErrInvalidArg
        assert h.launched == [], "关卡解析失败却仍然拉起了 DS"
    finally:
        await h.aclose()


async def test_local_allocate_fails_closed_when_token_signing_fails_under_enforce(
    local,
) -> None:
    """★ `enforce` 下签发失败 → `ErrDSAllocationFailed`,不拉进程,且端口不被吃掉。

    拉起一个无令牌的 DS 等于开了一局"连不回来"的对局:它的每一次回调都会被 enforce
    守卫拒掉,玩家进了图却什么都做不了。端口那条断言同样重要 —— 失败路径漏还端口的话,
    连续几次签发抖动就能把整个端口池耗干。

    ★ 变异:把 `allocate` 里 `if self._ds_token_required: raise ...` 改成只告警
      → 本条红。
    """

    async def boom(*_: object) -> tuple[str, LA.BattleCredentialIdentity]:
        raise RuntimeError("signing key unavailable")

    local.alloc.set_ds_token_issuer(boom, True)
    with pytest.raises(errcode.PandoraError) as got:
        await local.alloc.allocate(_MATCH_ID, _MAP_ID, _GAME_MODE, releasetrack.STABLE)
    assert got.value.code == errcode.ErrDSAllocationFailed
    assert local.launched == []

    # 端口没被失败路径吃掉:换成能签发的 issuer 后,第一个端口仍可用。
    local.alloc.set_ds_token_issuer(None, False)
    _, addr, _ = await local.alloc.allocate(_MATCH_ID, _MAP_ID, _GAME_MODE, releasetrack.STABLE)
    assert addr == "127.0.0.1:47000"


async def test_local_allocate_refuses_an_incomplete_credential_tuple_under_enforce(
    local,
) -> None:
    """★ 签发"成功"但凭据五元组不全,`enforce` 下同样必须在拉起**之前**失败。

    半截凭据意味着心跳应答回显不出绑定式 ACK,DS 会拒收 allocator 的 stop / 驱逐指令
    并永远拿不到本地准入租约 —— 一个每 5s 打一条 "ACK missing" 却谁也不管的 DS。
    这正是 §14 说的"半成品接线"。

    ★ 变异:把 `allocate` 里 `if not cred.complete_for_ack() or ...` 那段整块删掉
      → 本条红。
    """

    async def half(
        match_id: int, pod: str, uid: str, epoch: int
    ) -> tuple[str, LA.BattleCredentialIdentity]:
        # jti 缺失 → complete_for_ack() 为 False。
        return "tok", LA.BattleCredentialIdentity(
            instance_uid=uid, instance_epoch=epoch, gen=1, jti="", writer_epoch=2
        )

    local.alloc.set_ds_token_issuer(half, True)
    with pytest.raises(errcode.PandoraError) as got:
        await local.alloc.allocate(_MATCH_ID, _MAP_ID, _GAME_MODE, releasetrack.STABLE)
    assert got.value.code == errcode.ErrDSAllocationFailed
    assert local.launched == []


async def test_local_credential_ack_is_fail_closed_for_unknown_pods(local) -> None:
    """★ 心跳 ACK 回显严格 fail-closed:pod 不在台账 → `None`,绝不糊一个半截身份。

    ACK 的值必须来自"本进程签发、并经 env 下发给该 DS 的同一份凭据"。台账里没有,
    就说明这台 DS 已经不归本进程管;回一个"看起来像"的身份,等于替一台已被回收的
    实例背书。同理 `local_instance_identity`。

    ★ 变异:把 `local_credential_ack` 里 `if lp is None or not lp.cred.complete_for_ack(): return None`
      改成 `return lp.cred if lp else None` → 本条红(会回一个不完整凭据)。
    """

    async def issue(
        match_id: int, pod: str, uid: str, epoch: int
    ) -> tuple[str, LA.BattleCredentialIdentity]:
        return "tok", LA.BattleCredentialIdentity(
            instance_uid=uid, instance_epoch=epoch, gen=9, jti="jti-9", writer_epoch=2
        )

    local.alloc.set_ds_token_issuer(issue, False)
    pod, _, _ = await local.alloc.allocate(_MATCH_ID, _MAP_ID, _GAME_MODE, releasetrack.STABLE)

    ack = local.alloc.local_credential_ack(pod)
    assert ack is not None
    assert (ack.pod_name, ack.gen, ack.jti) == (pod, 9, "jti-9")
    assert local.alloc.local_instance_identity(pod) == (ack.instance_uid, ack.instance_epoch)

    assert local.alloc.local_credential_ack("pandora-battle-local-does-not-exist") is None
    assert local.alloc.local_credential_ack("") is None
    assert local.alloc.local_instance_identity("pandora-battle-local-nope") is None

    # 回收后立刻停止回显 —— 否则一台已被 kill 的 DS 还能拿到"服务端仍授权你"的证据。
    await local.alloc.release(pod)
    assert local.alloc.local_credential_ack(pod) is None


async def test_local_release_is_idempotent_and_frees_the_port(local) -> None:
    """★ `release` 幂等,且必须真的把端口还回池子。

    台账无此记录视作已释放(Go 注释原话)。不幂等的话,一次"崩溃 reaper 已清 + 上游
    再 Release"就会抛错,把一条正常的清理路径变成告警噪音。

    ★ 变异:把 `release` 里 `self._used_ports.discard(lp.port)` 删掉 → 本条红。
    """
    pod, addr, _ = await local.alloc.allocate(_MATCH_ID, _MAP_ID, _GAME_MODE, releasetrack.STABLE)
    await local.alloc.release(pod)
    assert local.procs[0].killed is True
    await local.alloc.release(pod)  # 幂等:第二次不许抛

    _, again, _ = await local.alloc.allocate(
        _MATCH_ID + 1, _MAP_ID, _GAME_MODE, releasetrack.STABLE
    )
    assert again == addr, "释放后端口没有回到池子"


@pytest.mark.parametrize(
    ("key", "reserved"),
    [
        ("PANDORA_DS_TOKEN", True),
        ("pandora_ds_token", True),  # Windows env 名大小写不敏感
        ("  PANDORA_MATCH_ID  ", True),
        ("AGONES_GAMESERVER_NAME", True),
        ("PANDORA_EXTRA_TUNABLE", False),
    ],
)
def test_reserved_ds_env_keys_are_matched_case_insensitively(key: str, reserved: bool) -> None:
    """★ `extra_env` 不得覆盖内置身份 / 令牌变量,且判定**大小写不敏感 + 先 strip**。

    local 模式的宿主是 Windows,环境变量名大小写不敏感:`pandora_ds_token` 与
    `PANDORA_DS_TOKEN` 指向同一个变量。只按精确大写比对的话,一个小写别名就能用
    静态 / 伪造令牌替换真签发令牌,绕过整套范围绑定(Go 审核 P1 补漏)。

    ★ 变异:把 `is_reserved_ds_env_key` 的 `key.strip().upper()` 改成 `key`
      → 小写与带空格那两条参数红。
    """
    assert LA.is_reserved_ds_env_key(key) is reserved
