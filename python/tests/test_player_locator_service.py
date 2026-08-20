"""player_locator 服务形态测试:配置默认值 / 12 道启动闸 / service 层返回形态。

分工:
  - tests/test_player_locator.py  —— 纯 biz(TTL 下限、入参校验、uint64 比较)
  - 本文件                        —— conf.Defaults 与 Go 对齐、main 的启动闸、
                                     service 层的 in-band code 形态、守卫状态机

★ fixture 里的 yaml **除被测项外必须处处合法**。踩过的坑:省了 node_id / redis host
  之类的字段,结果请求在走到被测的闸之前先被别的闸拦下 —— 测试通过了,
  但测的是另一道闸。所以下面统一从 `_base_yaml()` 起,只改被测的那一项。
"""

from __future__ import annotations

import asyncio
import pathlib
import re
import textwrap

import pytest
from pandora.common.v1 import errcode_pb2
from pandora.locator.v1 import locator_pb2

from pandorapy import errcode
from pandorapy.services.player_locator import biz as lbiz
from pandorapy.services.player_locator import conf as lconf
from pandorapy.services.player_locator import main as lmain
from pandorapy.services.player_locator import presence as lpresence
from pandorapy.services.player_locator import repo as lrepo
from pandorapy.services.player_locator import service as lsvc
from pandorapy.services.player_locator import usecase as lusecase

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
GO_SERVICE_DIR = REPO_ROOT / "services" / "runtime" / "player_locator"
GO_CONF = GO_SERVICE_DIR / "internal" / "conf" / "conf.go"
GO_MAIN = GO_SERVICE_DIR / "cmd" / "locator" / "main.go"
GO_SERVICE = GO_SERVICE_DIR / "internal" / "service" / "locator.go"
DEV_YAML = GO_SERVICE_DIR / "etc" / "locator-dev.yaml"


# ── 配置:与 Go 的 Defaults() 逐字段对齐 ─────────────────────────────────────


def test_real_dev_yaml_loads() -> None:
    """★ 必须能吃**同一份** etc/locator-dev.yaml —— 迁移期只维护一份配置。"""
    cfg = lconf.Config.load(str(DEV_YAML))
    assert cfg.server.grpc.addr == ":20006"
    assert cfg.server.http.addr == ":21006"
    assert cfg.locator.location_ttl == "30s"
    assert cfg.locator.last_seen_retention == "1h"
    assert cfg.locator.departure_event.enabled is True
    assert cfg.kafka.brokers == ["127.0.0.1:9093"]
    assert cfg.ds_auth.mode == "off"
    assert cfg.ds_auth.authority_mode == "legacy"


def test_defaults_match_go_source() -> None:
    """默认值直接对着 Go 源码断言 —— 任何一侧漂移当场变红。

    默认值分叉的后果不是"某一边报错",而是**同一份 yaml 在两个实现上跑出不同行为
    且两边都不报错**。端口尤其致命:Envoy cluster 钉在 20006/21006。
    """
    src = GO_CONF.read_text(encoding="utf-8")
    assert 'c.Server.Grpc.Addr = ":20006"' in src
    assert 'c.Server.Http.Addr = ":21006"' in src
    assert "config.Duration(30 * time.Second)" in src  # location_ttl
    assert "config.Duration(time.Hour)" in src  # last_seen_retention
    assert "config.Duration(8 * time.Second)" in src  # debounce_window
    assert "config.Duration(1 * time.Second)" in src  # coalesce_tick
    assert 'c.Presence.KillSwitchKey = "presence/fanout"' in src

    cfg = lconf.Config.model_validate({})
    cfg.apply_defaults()
    assert cfg.server.grpc.addr == lconf.DEFAULT_GRPC_ADDR == ":20006"
    assert cfg.server.http.addr == lconf.DEFAULT_HTTP_ADDR == ":21006"
    assert cfg.locator.location_ttl == "30s"
    assert cfg.locator.last_seen_retention == "1h"
    assert cfg.presence.debounce_window == "8s"
    assert cfg.presence.coalesce_tick == "1s"
    assert cfg.presence.kill_switch_key == "presence/fanout"
    assert cfg.ds_auth.authority_mode == "legacy"
    assert cfg.ds_auth.issuer == "pandora-ds-control"
    assert cfg.ds_auth.audience == "pandora-ds"
    assert cfg.ds_auth.active_heartbeat_max_age == "30s"


def test_go_defaults_use_equals_zero_not_le_zero() -> None:
    """★ 判据符号本身是契约:Go 用 `== 0`,不是 `<= 0`。

    差别在负值上:`location_ttl: "-1s"` 在 Go 侧**不会**被兜成 30s。
    Python 若写成 `<= 0`,同一份 yaml 会让两个实现拿到不同的 TTL,而且都不报错。
    """
    src = GO_CONF.read_text(encoding="utf-8")
    assert "if c.Locator.LocationTTL == 0 {" in src
    assert "if c.Locator.LastSeenRetention == 0 {" in src

    # 零值(缺字段 / 空串)才被兜默认。
    cfg = lconf.Config.model_validate({"locator": {"location_ttl": ""}})
    cfg.apply_defaults()
    assert cfg.locator.location_ttl == "30s"


def test_negative_duration_matches_go_instead_of_being_rejected() -> None:
    """★ 负 duration 现在与 Go **同解**（2026-08-19 修正，此前是一处两栈分叉）。

    原来这条断言 Python 会 `ValueError` 拒启，理由写的是"Go 会被机械下限抬到 27s、
    Python 拒启，两边都响"。逐条对 Go 之后发现**那个理由是错的**：

        Go `NewLocatorUsecase`（biz/locator.go:168-170）先执行 `if ttl <= 0 { ttl = 30s }`，
        30s 已经在 27s 屏障之上，所以 `-1s` 在 Go 侧的结果是 **30s，不是 27s**。

    而 Python 侧当时是 `parse_duration` 根本不认负号 → 同一份 yaml
    **Go 起得来、Python 启动即崩**，那不是"两边都响"，是一处真分叉。

    共享件已支持负号（Go 的 `time.ParseDuration("-1s")` 本来就合法），
    现在两栈都走 `<= 0 → 默认 30s`，逐值相同。
    """
    cfg = lconf.Config.model_validate({"locator": {"location_ttl": "-1s"}})
    cfg.apply_defaults()
    configured = int(cfg.locator.location_ttl_td().total_seconds())
    assert configured == -1, "负号没被解析出来 —— 共享件的负号支持掉了"
    # 与 Go biz/locator.go:168-170 同一条兜底：<=0 一律取默认 30s
    assert lbiz.effective_ttl_sec(configured) == lbiz.DEFAULT_TTL_SEC == 30
    # 且它在 27s 屏障之上，不会再被抬（抬了说明默认值被改小到危险区）
    assert lbiz.DEFAULT_TTL_SEC >= lbiz.placement.DS_FENCE_REENTRY_BARRIER_SECONDS


def test_zero_ttl_is_clamped_by_the_mechanical_floor_not_by_defaults() -> None:
    """usecase 侧的 TTL 下限是**正确性下限**,与 conf 的默认值是两道独立的兜底。"""
    uc = lusecase.LocatorUsecase(_FakeRepo(), 5.0)
    assert uc.ttl_sec == float(lbiz.effective_ttl_sec(5))


@pytest.mark.parametrize("mode", ["", "legacy", "redis"])
def test_authority_mode_accepted(mode: str) -> None:
    cfg = lconf.Config.model_validate({"ds_auth": {"authority_mode": mode}})
    cfg.apply_defaults()
    cfg.validate_ds_auth_authority_mode()  # 不抛


@pytest.mark.parametrize("mode", ["Redis", "reids", "on", "enforce"])
def test_authority_mode_typo_rejected(mode: str) -> None:
    """拼错必须拒启:静默退化为 legacy 会让终态门被绕过而启动日志毫无痕迹。"""
    cfg = lconf.Config.model_validate({"ds_auth": {"authority_mode": mode}})
    cfg.apply_defaults()
    with pytest.raises(ValueError, match="authority_mode invalid"):
        cfg.validate_ds_auth_authority_mode()


def test_validate_redis_fence_conditions() -> None:
    """authority_mode=redis 的五项前置逐条对齐 Go 的 ValidateRedisFence。"""
    base = {
        "mode": "enforce",
        "authority_mode": "redis",
        "active_heartbeat_max_age": "30s",
        "fence": {"etcd_endpoints": ["127.0.0.1:2380"], "keyset_revision": "r1"},
    }

    def build(**over):
        d = dict(base)
        d.update(over)
        c = lconf.Config.model_validate({"ds_auth": d})
        c.apply_defaults()
        return c.ds_auth

    build().validate_redis_fence()  # 全齐 → 通过

    with pytest.raises(ValueError, match="requires mode=enforce"):
        build(mode="permissive").validate_redis_fence()
    with pytest.raises(ValueError, match="fence.etcd_endpoints"):
        build(fence={"keyset_revision": "r1"}).validate_redis_fence()
    with pytest.raises(ValueError, match="keyset_revision"):
        build(fence={"etcd_endpoints": ["127.0.0.1:2380"]}).validate_redis_fence()

    # legacy 档下这条永远放行(不误伤 dev)。
    build(authority_mode="legacy").validate_redis_fence()


# ── 启动闸 ─────────────────────────────────────────────────────────────────


def _base_yaml() -> str:
    """一份**除被测项外处处合法**的最小配置。"""
    return textwrap.dedent(
        """
        server:
          grpc:
            addr: ":20006"
          http:
            addr: ":21006"
        node:
          node_id: 1
          redis_client:
            host: "127.0.0.1:6380"
            db: 0
        locator:
          location_ttl: "30s"
          last_seen_retention: "1h"
          departure_event:
            enabled: false
        kafka:
          brokers: ["127.0.0.1:9093"]
          partition_cnt: 4
        presence:
          enabled: false
        ds_auth:
          mode: "off"
          authority_mode: "legacy"
        """
    ).lstrip()


class _FakeRedis:
    async def aclose(self) -> None:
        return None


class _FakeProducer:
    def __init__(self, *_a, **_kw) -> None:
        pass

    async def close(self) -> None:
        return None


def _write(tmp_path: pathlib.Path, body: str) -> pathlib.Path:
    p = tmp_path / "locator.yaml"
    p.write_text(body, encoding="utf-8")
    return p


def _run_main(conf_path: pathlib.Path) -> int:
    return asyncio.run(lmain._main_async(lmain._parse_args(["-conf", str(conf_path)])))


@pytest.fixture()
def stub_deps(monkeypatch: pytest.MonkeyPatch) -> dict:
    """把 Redis / Kafka / server.run 换成桩,只留启动闸本身。"""
    state: dict = {"ready": False, "background": []}

    async def fake_connect(_conf, **_kw):
        return _FakeRedis()

    async def fake_run(**kw):
        state["ready"] = True
        state["background"] = list(kw.get("background") or [])
        if kw.get("on_ready"):
            kw["on_ready"]()

    monkeypatch.setattr(lmain.redisx, "must_connect", fake_connect)
    monkeypatch.setattr(lmain.pserver, "run", fake_run)
    monkeypatch.setattr(lmain.kafkax, "KeyOrderedProducer", _FakeProducer)
    return state


def test_gate_config_load_failed(tmp_path: pathlib.Path, stub_deps: dict) -> None:
    assert _run_main(tmp_path / "does-not-exist.yaml") == 1
    assert stub_deps["ready"] is False


def test_gate_config_scan_failed(tmp_path: pathlib.Path, stub_deps: dict) -> None:
    """结构对不上 → 拒启(这里用非 mapping 根节点)。"""
    p = _write(tmp_path, "- not-a-mapping\n")
    assert _run_main(p) == 1


def test_gate_cellroute_init_failed(tmp_path: pathlib.Path, stub_deps: dict) -> None:
    """★ 配了 cell_route.mode 必须拒启,不能静默按单 Cell 跑。

    Python 侧这道闸挂在 BaseConf 的校验器上(比 Go 早),但方向与事件名一致。
    """
    p = _write(tmp_path, _base_yaml() + 'cell_route:\n  mode: "hash"\n')
    assert _run_main(p) == 1


def test_gate_ds_auth_authority_mode_invalid(
    tmp_path: pathlib.Path, stub_deps: dict
) -> None:
    p = _write(tmp_path, _base_yaml().replace('authority_mode: "legacy"', 'authority_mode: "reids"'))
    assert _run_main(p) == 1


def test_gate_redis_endpoint_required(tmp_path: pathlib.Path, stub_deps: dict) -> None:
    """host / addrs 皆空 → 拒启;**绝不回落 127.0.0.1**(会连上无关的本机 Redis)。"""
    p = _write(tmp_path, _base_yaml().replace('host: "127.0.0.1:6380"', 'host: ""'))
    assert _run_main(p) == 1


def test_gate_redis_ping_failed(
    tmp_path: pathlib.Path, stub_deps: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def boom(_conf, **_kw):
        raise ConnectionError("redis down")

    monkeypatch.setattr(lmain.redisx, "must_connect", boom)
    assert _run_main(_write(tmp_path, _base_yaml())) == 1


def test_gate_departure_event_enabled_but_no_kafka(
    tmp_path: pathlib.Path, stub_deps: dict
) -> None:
    """★ 开了离场事件却没 broker 必须**拒启**,不能像 presence 那样降级。

    消费方的时效按「有事件」设计,producer 静默不可用会让整条链看起来在跑却永不
    触发 —— 这种失败模式比起不来更糟。
    """
    body = _base_yaml().replace(
        "  departure_event:\n    enabled: false", "  departure_event:\n    enabled: true"
    ).replace('brokers: ["127.0.0.1:9093"]', "brokers: []")
    assert _run_main(_write(tmp_path, body)) == 1


def test_gate_departure_event_producer_init_failed(
    tmp_path: pathlib.Path, stub_deps: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    class Boom:
        def __init__(self, *_a, **_kw):
            raise RuntimeError("no broker")

    monkeypatch.setattr(lmain.kafkax, "KeyOrderedProducer", Boom)
    body = _base_yaml().replace(
        "  departure_event:\n    enabled: false", "  departure_event:\n    enabled: true"
    )
    assert _run_main(_write(tmp_path, body)) == 1


@pytest.mark.parametrize("mode", ["permissive", "enforce"])
def test_gate_ds_auth_guard_init_failed(
    tmp_path: pathlib.Path, stub_deps: dict, mode: str
) -> None:
    """★ Python 没有 DS 回调令牌守卫 → mode!=off 必须**拒启**。

    静默当成 off 继续跑 = 把 fail-closed 的令牌校验降级成 fail-open:
    任何能到达 :8444 的东西都能改别人的位置投影,而 yaml 写着 enforce。
    """
    p = _write(tmp_path, _base_yaml().replace('mode: "off"', f'mode: "{mode}"'))
    assert _run_main(p) == 1
    assert stub_deps["ready"] is False


def test_presence_without_kafka_is_only_a_warning(
    tmp_path: pathlib.Path, stub_deps: dict
) -> None:
    """★ 方向不能改:presence 是可降级增强,没 broker 只 WARN + 退纯拉,**照常启动**。

    改成 fail-fast 会让一个好友面板的推送优化把整个 presence 主链路拖停。
    """
    body = _base_yaml().replace(
        "presence:\n  enabled: false", "presence:\n  enabled: true"
    ).replace('brokers: ["127.0.0.1:9093"]', "brokers: []")
    assert _run_main(_write(tmp_path, body)) == 0
    assert stub_deps["ready"] is True
    assert stub_deps["background"] == []  # 纯拉:不起 fan-out tick


def test_presence_producer_failure_is_only_a_warning(
    tmp_path: pathlib.Path, stub_deps: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    class Boom:
        def __init__(self, *_a, **_kw):
            raise RuntimeError("no broker")

    monkeypatch.setattr(lmain.kafkax, "KeyOrderedProducer", Boom)
    body = _base_yaml().replace("presence:\n  enabled: false", "presence:\n  enabled: true")
    assert _run_main(_write(tmp_path, body)) == 0
    assert stub_deps["ready"] is True


def test_happy_path_starts_and_registers_presence_loop(
    tmp_path: pathlib.Path, stub_deps: dict
) -> None:
    body = _base_yaml().replace("presence:\n  enabled: false", "presence:\n  enabled: true")
    assert _run_main(_write(tmp_path, body)) == 0
    assert stub_deps["ready"] is True
    # presence 开启且 producer 建成 → 起 fan-out tick 后台循环。
    assert [f.__name__ for f in stub_deps["background"]] == ["presence_fanout_tick"]


def test_default_config_starts_with_no_background_loop(
    tmp_path: pathlib.Path, stub_deps: dict
) -> None:
    assert _run_main(_write(tmp_path, _base_yaml())) == 0
    assert stub_deps["background"] == []


def test_gate_event_names_exist_in_go_main() -> None:
    """★ 事件名逐字对齐 Go —— Loki 告警和运维手册都按事件名建。"""
    src = GO_MAIN.read_text(encoding="utf-8")
    for event in (
        "abs_conf_path_failed",
        "config_load_failed",
        "config_scan_failed",
        "ds_auth_authority_mode_invalid",
        "ds_auth_fence_config_invalid",
        "redis_endpoint_required",
        "redis_ping_failed",
        "presence_enabled_but_no_kafka",
        "presence_producer_init_failed",
        "departure_event_enabled_but_no_kafka",
        "departure_event_producer_init_failed",
        "cellroute_init_failed",
        "ds_auth_guard_init_failed",
        "ds_auth_fence_acquire_failed",
        "service_ready",
    ):
        assert f'"{event}"' in src, f"Go main.go 里找不到事件名 {event}"
        assert event in lmain.__doc__ or event in pathlib.Path(
            lmain.__file__
        ).read_text(encoding="utf-8"), f"Python main.py 里没有事件名 {event}"


# ── service 层:返回形态 ────────────────────────────────────────────────────


class _FakeRepo:
    """只实现 usecase 用到的方法;每个方法都记账,便于断言副作用。"""

    def __init__(self) -> None:
        self.records: dict[int, lrepo.LocationRecord] = {}
        self.validate_result = True
        self.commit_result = True
        self.shrink_result = (True, True)
        self.touched: list[int] = []
        self.deleted: list[int] = []
        self.last_seen: dict[int, int] = {}

    async def validate_hub_presence(self, player_id, fence):  # noqa: ANN001
        return self.validate_result

    async def activate_hub_presence(self, player_id, fence, retention_sec):  # noqa: ANN001
        return self.commit_result

    async def set_guarded(self, player_id, rec, ttl_sec, max_retry, guard):  # noqa: ANN001
        cur = self.records.get(player_id)
        guard(cur or lrepo.LocationRecord(), cur is not None)
        self.records[player_id] = rec

    async def get(self, player_id):  # noqa: ANN001
        rec = self.records.get(player_id)
        return (rec, True) if rec is not None else (lrepo.LocationRecord(), False)

    async def batch_get(self, player_ids):  # noqa: ANN001
        return {p: self.records[p] for p in player_ids if p in self.records}

    async def batch_get_last_seen(self, player_ids):  # noqa: ANN001
        return {p: self.last_seen[p] for p in player_ids if p in self.last_seen}

    async def refresh_hub_locations(self, hub_pod, player_ids, ttl, meta_ttl):  # noqa: ANN001
        return len(player_ids)

    async def shrink_hub_ttl(self, hub_pod, player_id, fence, grace):  # noqa: ANN001
        return self.shrink_result

    async def record_last_seen(self, player_id, fence, at_ms, retention):  # noqa: ANN001
        self.last_seen[player_id] = at_ms
        return True, at_ms

    async def touch_alive(self, player_id, at_ms, retention):  # noqa: ANN001
        self.touched.append(player_id)

    async def delete(self, player_id):  # noqa: ANN001
        self.deleted.append(player_id)
        self.records.pop(player_id, None)


def _svc(repo: _FakeRepo | None = None) -> tuple[lsvc.LocatorService, _FakeRepo]:
    r = repo or _FakeRepo()
    return lsvc.LocatorService(lusecase.LocatorUsecase(r, 30.0)), r


PLACEMENT_STUBS = (
    "GetPlacement",
    "BeginPlacementTransition",
    "BindPlacementTarget",
    "ConfirmPlacementSourceDeparture",
    "RetargetPlacementTarget",
    "CommitPlacementAdmission",
    "BootstrapPlacement",
)


@pytest.mark.parametrize("rpc", PLACEMENT_STUBS)
def test_placement_rpcs_are_disabled_stubs(rpc: str) -> None:
    """★ 7 个 placement RPC 是 Go 侧**刻意下线的 stub**,照抄同一个码。

    "顺手实现"会让已下线的路由权威重新活过来 —— 路由权威现在只有 TTL 位置租约。
    """
    svc, _ = _svc()
    req = getattr(locator_pb2, f"{rpc}Request")()
    resp = asyncio.run(getattr(svc, rpc)(req, None))
    assert resp.code == errcode_pb2.ERR_SERVICE_DISABLED


def test_go_side_placement_stubs_are_still_stubs() -> None:
    """Go 侧一旦把某个 placement RPC 实现回来,本测试提醒 Python 侧同步。"""
    src = GO_SERVICE.read_text(encoding="utf-8")
    for rpc in PLACEMENT_STUBS:
        assert f'placementRemoved(ctx, "{rpc}")' in src, f"Go 侧 {rpc} 不再是 stub"


def test_business_failure_returns_in_band_code_not_grpc_error() -> None:
    """★ 业务失败返回 `Response(code=...)` 且 gRPC status 为 OK。

    改成 abort 会让调用方走到完全不同的错误分支 —— 迁移中最容易悄悄改掉的语义。
    """
    svc, _ = _svc()
    resp = asyncio.run(
        svc.SetLocation(locator_pb2.SetLocationRequest(player_id=0), None)
    )
    assert resp.code == errcode_pb2.ERR_INVALID_ARG


def test_get_location_miss_returns_offline_placeholder() -> None:
    """key miss = OFFLINE 占位(不报错)。

    ★ 但这只说明 presence 不可见,**不能**据此判定玩家已离开旧 DS(§9.22)。
    """
    svc, _ = _svc()
    resp = asyncio.run(svc.GetLocation(locator_pb2.GetLocationRequest(player_id=7), None))
    assert resp.code == errcode_pb2.OK
    assert resp.location.state == locator_pb2.LOCATION_STATE_OFFLINE


def test_batch_get_location_omits_missing_players() -> None:
    """批量查**不给 miss 回填占位** —— 缺席即离线,避免响应被离线占位撞胀。"""
    svc, repo = _svc()
    repo.records[11] = lrepo.LocationRecord(state=lbiz.LOCATION_STATE_HUB, hub_pod="hub-1")
    resp = asyncio.run(
        svc.BatchGetLocation(locator_pb2.BatchGetLocationRequest(player_ids=[11, 12]), None)
    )
    assert resp.code == errcode_pb2.OK
    assert set(resp.locations.keys()) == {11}


def test_set_location_hub_does_not_persist_match_id() -> None:
    """HUB 报文的 match_id 只是 BATTLE fence 令牌,不得落库(免其它服务误读)。"""
    svc, repo = _svc()
    req = locator_pb2.SetLocationRequest(
        player_id=5,
        location=locator_pb2.Location(
            state=locator_pb2.LOCATION_STATE_HUB, hub_pod="hub-1", match_id=99
        ),
    )
    resp = asyncio.run(svc.SetLocation(req, None))
    assert resp.code == errcode_pb2.OK
    assert repo.records[5].match_id == 0
    assert repo.records[5].battle_pod == ""


def test_stale_hub_presence_returns_locator_conflict() -> None:
    """代际闸拒绝 → ErrLocatorConflict(in-band),这是"秒重连被旧连接顶回"的唯一拦截点。"""
    svc, repo = _svc()
    repo.validate_result = False
    req = locator_pb2.SetLocationRequest(
        player_id=5,
        location=locator_pb2.Location(state=locator_pb2.LOCATION_STATE_HUB, hub_pod="hub-1"),
        hub_presence_fence=locator_pb2.HubPresenceFence(
            assignment_id="a1", admission_id="ad1", admission_seq=3
        ),
    )
    resp = asyncio.run(svc.SetLocation(req, None))
    assert resp.code == errcode.ErrLocatorConflict


def test_report_disconnect_without_fence_is_ok_but_noop() -> None:
    """★ 旧 Hub DS 没带 fence → 安全降级:RPC 仍 OK,但**什么都没做**。

    这条降级不是无害的(不缩 TTL / 不记 last-seen / 不发离场事件),
    所以 usecase 侧打 Error + 计数 —— 这里断言的是"不报错但 shrunk=False"。
    """
    svc, repo = _svc()
    resp = asyncio.run(
        svc.ReportDisconnect(
            locator_pb2.ReportDisconnectRequest(hub_pod="hub-1", player_id=5), None
        )
    )
    assert resp.code == errcode_pb2.OK
    assert resp.shrunk is False
    assert repo.last_seen == {}


def test_report_disconnect_partial_fence_rejected() -> None:
    """半齐 fence 是非法的,禁止把残缺身份冒充 legacy。"""
    svc, _ = _svc()
    resp = asyncio.run(
        svc.ReportDisconnect(
            locator_pb2.ReportDisconnectRequest(
                hub_pod="hub-1",
                player_id=5,
                hub_presence_fence=locator_pb2.HubPresenceFence(assignment_id="a1"),
            ),
            None,
        )
    )
    assert resp.code == errcode_pb2.ERR_INVALID_ARG


def test_report_disconnect_with_complete_fence_records_last_seen() -> None:
    svc, repo = _svc()
    resp = asyncio.run(
        svc.ReportDisconnect(
            locator_pb2.ReportDisconnectRequest(
                hub_pod="hub-1",
                player_id=5,
                hub_presence_fence=locator_pb2.HubPresenceFence(
                    assignment_id="a1", admission_id="ad1", admission_seq=3
                ),
            ),
            None,
        )
    )
    assert resp.code == errcode_pb2.OK
    assert resp.shrunk is True
    assert 5 in repo.last_seen


def test_refresh_hub_locations_requires_pod() -> None:
    """整批被拒 = 这台 Hub 上所有人的 presence 都不再续期,必须是显式错误码。"""
    svc, _ = _svc()
    resp = asyncio.run(
        svc.RefreshHubLocations(
            locator_pb2.RefreshHubLocationsRequest(hub_pod="", player_ids=[1, 2]), None
        )
    )
    assert resp.code == errcode_pb2.ERR_INVALID_ARG


def test_subscribe_presence_is_noop_when_fanout_disabled() -> None:
    """presence 未启用时订阅是 no-op(纯拉),**不报错** —— 与 Go 同。"""
    svc, _ = _svc()
    resp = asyncio.run(
        svc.SubscribePresence(
            locator_pb2.SubscribePresenceRequest(subscriber_id=9, watched_player_ids=[1]),
            None,
        )
    )
    assert resp.code == errcode_pb2.OK


# ── 状态机守卫(不变量 §1)──────────────────────────────────────────────────


def _cur(state: int, **kw) -> lrepo.LocationRecord:
    return lrepo.LocationRecord(state=state, **kw)


def _apply_guard(cur: lrepo.LocationRecord, found: bool, inp: lbiz.LocationInput) -> None:
    fence = lrepo.HubPresenceFence(
        assignment_id=inp.hub_presence_fence.assignment_id,
        admission_id=inp.hub_presence_fence.admission_id,
        admission_seq=inp.hub_presence_fence.admission_seq,
    )
    lusecase.guard_transition(inp, fence)(cur, found)


def test_guard_allows_first_write() -> None:
    _apply_guard(lrepo.LocationRecord(), False, lbiz.LocationInput(player_id=1, state=3))


def test_guard_rejects_hub_report_during_matching() -> None:
    """撮合确认期玩家物理上还连着 hub DS,放行会顶掉 matchmaker 刚写的 MATCHING。"""
    inp = lbiz.LocationInput(player_id=1, state=lbiz.LOCATION_STATE_HUB, hub_pod="h1")
    with pytest.raises(errcode.PandoraError) as ei:
        _apply_guard(_cur(lbiz.LOCATION_STATE_MATCHING, match_id=7), True, inp)
    assert ei.value.code == errcode.ErrLocatorConflict


def test_guard_allows_non_hub_write_during_matching() -> None:
    _apply_guard(
        _cur(lbiz.LOCATION_STATE_MATCHING, match_id=7),
        True,
        lbiz.LocationInput(player_id=1, state=lbiz.LOCATION_STATE_BATTLE, match_id=7),
    )


def test_guard_rejects_bare_login_evicting_active_battle() -> None:
    """★ 这是 battle-reconnect §5 修的核心洞。

    放行会让客户端反复重登把 BATTLE 冲成 LOGIN_PENDING,matchmaker 读到误判空闲
    → 一人两处(破 §1)。一次裸登录本就不该有权终止一场进行中的战斗。
    """
    inp = lbiz.LocationInput(player_id=1, state=lbiz.LOCATION_STATE_LOGIN_PENDING)
    with pytest.raises(errcode.PandoraError):
        _apply_guard(_cur(lbiz.LOCATION_STATE_BATTLE, match_id=7), True, inp)


def test_guard_rejects_battle_write_for_different_match() -> None:
    inp = lbiz.LocationInput(
        player_id=1, state=lbiz.LOCATION_STATE_BATTLE, match_id=8, battle_pod="b1"
    )
    with pytest.raises(errcode.PandoraError):
        _apply_guard(_cur(lbiz.LOCATION_STATE_BATTLE, match_id=7), True, inp)


def test_guard_allows_same_match_battle_heartbeat() -> None:
    inp = lbiz.LocationInput(
        player_id=1, state=lbiz.LOCATION_STATE_BATTLE, match_id=7, battle_pod="b1"
    )
    _apply_guard(_cur(lbiz.LOCATION_STATE_BATTLE, match_id=7), True, inp)


def test_guard_hub_return_from_battle_needs_match_token() -> None:
    """打完回大厅必须带当前战斗的 match_id 令牌;不带 = 不知道 active BATTLE 的 stale hub。"""
    ok = lbiz.LocationInput(
        player_id=1, state=lbiz.LOCATION_STATE_HUB, hub_pod="h1", match_id=7
    )
    _apply_guard(_cur(lbiz.LOCATION_STATE_BATTLE, match_id=7), True, ok)

    bad = lbiz.LocationInput(player_id=1, state=lbiz.LOCATION_STATE_HUB, hub_pod="h1")
    with pytest.raises(errcode.PandoraError):
        _apply_guard(_cur(lbiz.LOCATION_STATE_BATTLE, match_id=7), True, bad)


def test_guard_rejects_legacy_hub_write_over_fenced_presence() -> None:
    """已经有带 fence 的当前连接时,不接受不带 fence 的写把它降级回"说不清哪条连接"。"""
    cur = _cur(
        lbiz.LOCATION_STATE_HUB,
        hub_pod="h1",
        hub_presence_fence=lrepo.HubPresenceFence("a1", "ad2", 2),
    )
    inp = lbiz.LocationInput(player_id=1, state=lbiz.LOCATION_STATE_HUB, hub_pod="h1")
    with pytest.raises(errcode.PandoraError):
        _apply_guard(cur, True, inp)


def test_guard_rejects_stale_admission_seq_in_same_assignment() -> None:
    """同 assignment 内 seq 回退 = 旧连接迟到的 SetLocation 想反向夺回投影。"""
    cur = _cur(
        lbiz.LOCATION_STATE_HUB,
        hub_pod="h1",
        hub_presence_fence=lrepo.HubPresenceFence("a1", "ad2", 2),
    )
    inp = lbiz.LocationInput(
        player_id=1,
        state=lbiz.LOCATION_STATE_HUB,
        hub_pod="h1",
        hub_presence_fence=lbiz.HubPresenceFence("a1", "ad1", 1),
    )
    with pytest.raises(errcode.PandoraError):
        _apply_guard(cur, True, inp)


def test_guard_rejects_same_seq_different_admission_id_aba() -> None:
    cur = _cur(
        lbiz.LOCATION_STATE_HUB,
        hub_pod="h1",
        hub_presence_fence=lrepo.HubPresenceFence("a1", "ad2", 2),
    )
    inp = lbiz.LocationInput(
        player_id=1,
        state=lbiz.LOCATION_STATE_HUB,
        hub_pod="h1",
        hub_presence_fence=lbiz.HubPresenceFence("a1", "ad-other", 2),
    )
    with pytest.raises(errcode.PandoraError):
        _apply_guard(cur, True, inp)


def test_guard_allows_cross_assignment_write() -> None:
    """★ 跨 assignment **刻意不定序** —— 归属是 hub_allocator 的权威,locator 是投影。

    要在这里反向定序就得实时查 owner,等于让「进大厅写位置」强依赖另一个服务。
    """
    cur = _cur(
        lbiz.LOCATION_STATE_HUB,
        hub_pod="h1",
        hub_presence_fence=lrepo.HubPresenceFence("a1", "ad9", 9),
    )
    inp = lbiz.LocationInput(
        player_id=1,
        state=lbiz.LOCATION_STATE_HUB,
        hub_pod="h2",
        hub_presence_fence=lbiz.HubPresenceFence("a2", "ad1", 1),
    )
    _apply_guard(cur, True, inp)


# ── presence fan-out ───────────────────────────────────────────────────────


class _RecordingPusher:
    def __init__(self) -> None:
        self.pushed: list[tuple[int, list]] = []

    async def push_presence(self, subscriber_id, changes):  # noqa: ANN001
        self.pushed.append((subscriber_id, changes))


def test_presence_debounce_absorbs_flap() -> None:
    """★ 窗口内回到原状态 = 抖动,不推(§13.4.2)。"""
    clock = {"t": 0.0}
    pusher = _RecordingPusher()
    hub = lpresence.PresenceHub(pusher, 8.0, 1.0, None, clock=lambda: clock["t"])
    hub.subscribe(100, [1])

    hub.notify(1, lbiz.LOCATION_STATE_HUB)  # 上线
    clock["t"] = 1.0
    hub.notify(1, lbiz.LOCATION_STATE_OFFLINE)  # 窗口内又下线
    clock["t"] = 9.0
    asyncio.run(hub.step())
    assert pusher.pushed == []  # 净变化为 0 → 不推


def test_presence_coalesces_and_pushes_after_window() -> None:
    clock = {"t": 0.0}
    pusher = _RecordingPusher()
    hub = lpresence.PresenceHub(pusher, 8.0, 1.0, None, clock=lambda: clock["t"])
    hub.subscribe(100, [1, 2])
    hub.notify(1, lbiz.LOCATION_STATE_HUB)
    hub.notify(2, lbiz.LOCATION_STATE_BATTLE)
    clock["t"] = 9.0
    asyncio.run(hub.step())
    assert len(pusher.pushed) == 1  # 同订阅者的两条合并成一批
    sub, changes = pusher.pushed[0]
    assert sub == 100
    assert {c.player_id: c.status for c in changes} == {
        1: lpresence.PRESENCE_ONLINE,
        2: lpresence.PRESENCE_IN_GAME,
    }


def test_presence_killswitch_degrades_to_pure_pull() -> None:
    """洪峰降级(§13.5):丢在途事件退回纯拉,保主链路。"""
    clock = {"t": 0.0}
    pusher = _RecordingPusher()
    hub = lpresence.PresenceHub(
        pusher, 8.0, 1.0, lambda: (True, "flood"), clock=lambda: clock["t"]
    )
    hub.subscribe(100, [1])
    hub.notify(1, lbiz.LOCATION_STATE_HUB)
    clock["t"] = 9.0
    asyncio.run(hub.step())
    assert pusher.pushed == []


def test_presence_status_encoding_matches_proto() -> None:
    """粗粒度状态直接引用生成物,不手抄(本仓刚修完 13 处手抄错位)。"""
    assert lpresence.PRESENCE_OFFLINE == locator_pb2.PRESENCE_STATUS_OFFLINE == 1
    assert lpresence.PRESENCE_ONLINE == locator_pb2.PRESENCE_STATUS_ONLINE == 2
    assert lpresence.PRESENCE_IN_GAME == locator_pb2.PRESENCE_STATUS_IN_GAME == 3


@pytest.mark.parametrize(
    "state,expected",
    [
        (lbiz.LOCATION_STATE_UNSPECIFIED, lpresence.PRESENCE_OFFLINE),
        (lbiz.LOCATION_STATE_OFFLINE, lpresence.PRESENCE_OFFLINE),
        (lbiz.LOCATION_STATE_LOGIN_PENDING, lpresence.PRESENCE_ONLINE),
        (lbiz.LOCATION_STATE_HUB, lpresence.PRESENCE_ONLINE),
        (lbiz.LOCATION_STATE_MATCHING, lpresence.PRESENCE_IN_GAME),
        (lbiz.LOCATION_STATE_BATTLE, lpresence.PRESENCE_IN_GAME),
    ],
)
def test_coarse_presence_mapping(state: int, expected: int) -> None:
    assert lpresence.coarse_presence(state) == expected


# ── repo:key 口径与 Lua 逐字一致 ───────────────────────────────────────────


def test_redis_keys_match_go() -> None:
    """★ key 前缀差一个字符 = 两个实现各写各的 key,§1 的顶号语义当场失效。"""
    src = (GO_SERVICE_DIR / "internal" / "data" / "location.go").read_text(encoding="utf-8")
    assert '"pandora:locator:%d"' in src
    assert '"pandora:locator:lastseen:%d"' in src
    assert '"pandora:locator:hubmeta:%d"' in src
    assert lrepo.loc_key(7) == "pandora:locator:7"
    assert lrepo.last_seen_key(7) == "pandora:locator:lastseen:7"
    assert lrepo.hub_meta_key(7) == "pandora:locator:hubmeta:7"


def _go_lua(src: str, var: str) -> str:
    m = re.search(rf"var {var} = redis\.NewScript\(`(.*?)`\)", src, re.S)
    assert m is not None, f"Go 侧找不到脚本 {var}"
    return m.group(1)


def _strip_comments(body: str) -> str:
    """去掉 Lua 注释和空白 —— 注释在 Go 侧是中文说明,不参与行为。"""
    out = []
    for line in body.split("\n"):
        line = line.split("--", 1)[0] if line.strip().startswith("--") else line
        if line.strip():
            out.append(line.strip())
    return "\n".join(out)


def test_lua_scripts_match_go() -> None:
    """★ 三段 Lua 必须与 Go **逐字一致**(注释除外)。

    灰度期两个实现同时打同一个 Redis;脚本行为差一点,对同一份 meta 的判定就会分叉
    ——「有的副本接受这次 HUB 写、有的拒绝」,没有任何一边报错。
    """
    src = (GO_SERVICE_DIR / "internal" / "data" / "location.go").read_text(encoding="utf-8")
    pairs = [
        ("hubPresenceScript", lrepo.HUB_PRESENCE_SCRIPT.body),
        ("touchHubAliveScript", lrepo.TOUCH_HUB_ALIVE_BODY),
        ("shrinkHubTTLScript", lrepo.SHRINK_HUB_TTL_SCRIPT.body),
        ("recordLastSeenScript", lrepo.RECORD_LAST_SEEN_SCRIPT.body),
    ]
    for go_var, py_body in pairs:
        assert _strip_comments(_go_lua(src, go_var)) == _strip_comments(py_body), (
            f"{go_var} 与 Python 侧不一致"
        )


def test_alive_touch_throttle_matches_go() -> None:
    src = (GO_SERVICE_DIR / "internal" / "data" / "location.go").read_text(encoding="utf-8")
    assert "AliveTouchThrottle = 30 * time.Second" in src
    assert lrepo.ALIVE_TOUCH_THROTTLE_SEC == 30


def test_disconnect_grace_matches_go() -> None:
    src = (GO_SERVICE_DIR / "internal" / "biz" / "locator.go").read_text(encoding="utf-8")
    assert "disconnectGrace = 10 * time.Second" in src
    assert lusecase.DISCONNECT_GRACE_SEC == 10.0


def test_optimistic_retry_matches_go() -> None:
    src = (GO_SERVICE_DIR / "internal" / "biz" / "locator.go").read_text(encoding="utf-8")
    assert "optimisticRetry = 3" in src
    assert lusecase.OPTIMISTIC_RETRY == 3
