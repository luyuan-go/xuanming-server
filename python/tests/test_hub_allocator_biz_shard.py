"""`biz_shard.py` 的行为测试 —— 对应 Go 侧 `internal/biz/hub_test.go` /
`hub_modelb_test.go` / `hub_owner_cleanup_test.go` / `hub_canary_test.go` /
`local_ticket_binding_test.go` 里覆盖「内部辅助」段的那些用例。

## 这批测试到底在守什么

被测代码是 §9 不变量 21 / 22 / 23 的执行体。它的失效形状**全都是静默的**:

  - 轨道筛选写松一格 → 已粘 canary 的玩家被甩回 stable,没有任何错误;
  - `assignment_same_instance` 少判一个字段 → 同名 GameServer 重建后,新实例
    "继承"了旧实例的全部在场玩家(脑裂),日志全绿;
  - saga 的补偿分支写反 → 要么座位泄漏(几小时后分片假满),要么把**已提交**的
    新 owner 的座位退掉(玩家被踢);
  - cleanup 的 `departure_required` 判据丢掉 → 容量账本清理被当成物理驱逐证明,
    旧 Hub 上那条活连接还在,于是同一玩家两台可玩 DS。

以上没有一条会在 happy path 上出现。所以本文件的重心是**否定路径**:逐字段
knockout、每个 census reason 分支、每条补偿分支、每种 cleanup 冲突。

## 测试口径

不连真 Redis / k8s / owner 服务:被测层是**判定 + 编排**,存储语义在 `repo.py` /
`ledger.py` / `auth_repo.py` 各有自己的测试。这里用能精确制造竞态返回的假件
(CAS 结果脚本化、seat 快照脚本化),把 Go 测试里那些"只在并发窗口出现"的分支
变成确定性用例。

每个用例的 `★ 变异:` 一行写明"把生产代码的哪一行改坏,本用例会红" ——
没有这一行的测试等于没验证过自己有没有牙。
"""

from __future__ import annotations

import asyncio
import dataclasses

import pytest
from pandora.hub.v1 import allocator_pb2 as hubpb

from pandorapy import errcode, releasetrack
from pandorapy.auth import DS_AUTH_WRITER_EPOCH_V2
from pandorapy.services.hub_allocator import biz_shard as bs
from pandorapy.services.hub_allocator import conf as hconf
from pandorapy.services.hub_allocator import fleet as F
from pandorapy.services.hub_allocator import ledger as L
from pandorapy.services.hub_allocator import repo as R
from pandorapy.services.hub_allocator.biz_base import (
    STATE_DRAINING,
    STATE_READY,
    STATE_WARMING,
    HubUsecaseBase,
)
from pandorapy.services.hub_allocator.owner_authority import OWNER_TYPE_HUB
from pandorapy.services.hub_allocator.owner_lease_client import (
    OwnerRecordView,
    OwnerTargetView,
)

PLAYER = 10086
POD = "pandora-hub-global-1"
UID = "uid-aaa"
AID = "assign-aaa"

pytestmark = pytest.mark.asyncio


# ── 工厂 ─────────────────────────────────────────────────────────────────────


def shard(
    pod: str = POD,
    *,
    region: str = "global",
    state: str = STATE_READY,
    track: str = releasetrack.STABLE,
    shard_id: int = 1,
    count: int = 0,
    cap: int = 10,
    addr: str = "1.2.3.4:7777",
    uid: str = "",
    epoch: int = 0,
    heartbeat_ms: int = 0,
    draining_since_ms: int = 0,
    token_gen: int = 0,
) -> hubpb.HubShardStorageRecord:
    return hubpb.HubShardStorageRecord(
        hub_pod_name=pod,
        hub_addr=addr,
        region=region,
        shard_id=shard_id,
        player_count=count,
        capacity=cap,
        state=state,
        release_track=track,
        gameserver_uid=uid,
        auth_epoch=epoch,
        last_heartbeat_ms=heartbeat_ms,
        draining_since_ms=draining_since_ms,
        current_token_gen=token_gen,
    )


def assignment(
    *,
    player_id: int = PLAYER,
    pod: str = POD,
    aid: str = AID,
    uid: str = UID,
    epoch: int = 7,
    gen: int = 3,
    jti: str = "jti-aaa",
    writer_epoch: int = DS_AUTH_WRITER_EPOCH_V2,
    track: str = releasetrack.STABLE,
    addr: str = "1.2.3.4:7777",
    shard_id: int = 1,
    region: str = "global",
    team_id: int = 0,
    role_id: int = 55,
    source_revision: int = 4242,
) -> hubpb.HubAssignmentStorageRecord:
    """一份**完整的 writer-v2** 归属记录(happy path 基线)。"""
    return hubpb.HubAssignmentStorageRecord(
        player_id=player_id,
        hub_pod_name=pod,
        hub_addr=addr,
        shard_id=shard_id,
        region=region,
        team_id=team_id,
        role_id=role_id,
        assignment_id=aid,
        hub_instance_uid=uid,
        auth_epoch=epoch,
        auth_gen=gen,
        auth_jti=jti,
        auth_writer_epoch=writer_epoch,
        release_track=track,
        source_revision=source_revision,
    )


def seat(
    *,
    ok: bool = True,
    uid: str = UID,
    epoch: int = 7,
    gen: int = 3,
    jti: str = "jti-aaa",
    writer_epoch: int = DS_AUTH_WRITER_EPOCH_V2,
    track: str = releasetrack.STABLE,
    addr: str = "9.9.9.9:7777",
    region: str = "global",
    shard_id: int = 1,
    count: int = 4,
    cap: int = 10,
    reason: str = "",
) -> L.ReserveResult:
    return L.ReserveResult(
        ok=ok,
        reason=reason,
        active_gen=gen,
        active_jti=jti,
        instance_uid=uid,
        protocol_epoch=epoch,
        writer_epoch=writer_epoch,
        shard_id=shard_id,
        hub_addr=addr,
        region=region,
        player_count=count,
        capacity=cap,
        release_track=track,
    )


def candidate(
    pod: str = POD,
    *,
    region: str = "global",
    shard_id: int = 1,
    cap: int = 10,
    track: str = releasetrack.STABLE,
    token_ready: bool = True,
    token_gen: int = 0,
    token_exp_ms: int = 0,
    uid: str = "",
    epoch: int = 0,
) -> F.ShardCandidate:
    return F.ShardCandidate(
        pod_name=pod,
        addr="1.2.3.4:7777",
        region=region,
        shard_id=shard_id,
        capacity=cap,
        release_track=track,
        token_ready=token_ready,
        token_exp_ms=token_exp_ms,
        token_gen=token_gen,
        instance_uid=uid,
        protocol_epoch=epoch,
    )


def target_of(a: hubpb.HubAssignmentStorageRecord) -> OwnerTargetView:
    return OwnerTargetView(
        pod_name=a.hub_pod_name,
        instance_uid=a.hub_instance_uid,
        instance_epoch=a.auth_epoch,
        assignment_or_allocation_id=a.assignment_id,
        release_track=a.release_track,
        source_revision=a.source_revision,
    )


# ── 假件 ─────────────────────────────────────────────────────────────────────


class FakeRepo:
    """脚本化的归属 / 分片仓储。返回 None 表示"不存在"(与 repo.py 的真实契约一致)。"""

    def __init__(
        self,
        shards: list | None = None,
        *,
        assignment_rec=None,
        team_shard: str | None = None,
    ) -> None:
        self.shards = list(shards or [])
        self.assignment = assignment_rec
        self.team_shard = team_shard
        self.created: list = []
        self.updated: list[str] = []
        self.cas_calls: list[tuple] = []
        # 依次弹出的 CAS 结果;空 = 恒 True。
        self.cas_results: list[bool] = []
        self.cas_error: BaseException | None = None
        self.registered: list[tuple[str, R.TransferCleanupRef]] = []
        self.removed_refs: list[tuple[str, R.TransferCleanupRef]] = []
        self.list_error: BaseException | None = None
        self.team_error: BaseException | None = None
        self.get_shard_error: BaseException | None = None
        self.remove_ref_error: BaseException | None = None

    async def list_shards(self) -> list:
        if self.list_error is not None:
            raise self.list_error
        return list(self.shards)

    async def get_shard(self, pod: str):
        if self.get_shard_error is not None:
            raise self.get_shard_error
        for s in self.shards:
            if s.hub_pod_name == pod:
                return s
        return None

    async def create_shard(self, rec, shard_ttl_sec: float) -> None:
        self.created.append(rec)
        self.shards.append(rec)

    async def update_shard_with_lock(self, pod: str, max_retry: int, fn, shard_ttl_sec: float):
        for s in self.shards:
            if s.hub_pod_name == pod:
                self.updated.append(pod)
                fn(s)
                return
        raise errcode.PandoraError(errcode.ErrHubNoAvailable, "hub shard %s not found", pod)

    async def get_team_shard(self, team_id: int) -> str | None:
        if self.team_error is not None:
            raise self.team_error
        return self.team_shard

    async def get_assignment(self, player_id: int):
        return self.assignment

    async def compare_and_swap_assignment(self, player_id, expected, next_rec, ttl):
        self.cas_calls.append((player_id, expected, next_rec, ttl))
        if self.cas_error is not None:
            raise self.cas_error
        ok = self.cas_results.pop(0) if self.cas_results else True
        if ok:
            self.assignment = next_rec
        return ok

    async def register_transfer_cleanup(self, source_pod: str, ref) -> None:
        self.registered.append((source_pod, ref))

    async def remove_transfer_cleanup(self, source_pod: str, ref) -> None:
        if self.remove_ref_error is not None:
            raise self.remove_ref_error
        self.removed_refs.append((source_pod, ref))


class FakeAuthRepo:
    """脚本化 Model B 权威面。"""

    def __init__(
        self,
        *,
        routable: L.ReserveResult | None = None,
        reserve: L.ReserveResult | None = None,
        release_ok: bool = True,
        release_error: BaseException | None = None,
        inspect: L.AssignmentSeatSnapshot | None = None,
        exact: L.ReleaseAssignmentSeatResult | None = None,
    ) -> None:
        self.routable = routable if routable is not None else seat()
        self.reserve = reserve if reserve is not None else seat()
        self.release_ok = release_ok
        self.release_error = release_error
        self.inspect_result = inspect or L.AssignmentSeatSnapshot(reserved=True)
        self.exact_result = exact or L.ReleaseAssignmentSeatResult(released=True)
        self.reserved: list[tuple[str, L.ReservationIdentity]] = []
        self.released: list[tuple[str, L.AssignmentInstanceIdentity]] = []
        self.exact_calls: list[tuple[str, L.AssignmentInstanceIdentity]] = []
        self.proofs: list[tuple[str, str]] = []
        self.routable_by_pod: dict[str, L.ReserveResult] = {}
        self.proof_error: BaseException | None = None

    async def check_routable(self, pod: str, at_ms: int, max_age_ms: int) -> L.ReserveResult:
        return self.routable_by_pod.get(pod, self.routable)

    async def reserve_assignment(
        self, pod, reservation, at_ms, max_heartbeat_age_ms, shard_ttl_sec
    ):
        self.reserved.append((pod, reservation))
        return self.reserve

    async def release_assignment_seat(self, pod, expected, shard_ttl_sec) -> bool:
        self.released.append((pod, expected))
        if self.release_error is not None:
            raise self.release_error
        return self.release_ok

    async def release_assignment_seat_exact(self, pod, expected, shard_ttl_sec):
        self.exact_calls.append((pod, expected))
        return self.exact_result

    async def inspect_assignment_seat(self, pod, expected):
        return self.inspect_result

    async def record_instance_teardown_proof(self, pod, instance_uid, proof_ttl_sec) -> None:
        if self.proof_error is not None:
            raise self.proof_error
        self.proofs.append((pod, instance_uid))


class FakeFleet:
    """只实现 `list_shards` 的拓扑源(刻意不实现 observer / scaler / 本地凭据源)。"""

    def __init__(self, cands: list | None = None, *, error: BaseException | None = None) -> None:
        self.cands = list(cands or [])
        self.error = error
        self.calls: list[str] = []

    async def list_shards(self, region: str) -> list:
        self.calls.append(region)
        if self.error is not None:
            raise self.error
        return [c for c in self.cands if c.region == region]


class ObservingFleet(FakeFleet):
    """叠加 `HubFleetPhysicalObserver`(对应 Go 的 fleet 类型断言成功那一支)。"""

    def __init__(self, cands=None, *, observation=None, observe_error=None) -> None:
        super().__init__(cands)
        self.observation = observation or F.HubInstanceObservation()
        self.observe_error = observe_error
        self.observed: list[str] = []

    async def observe_shard_instance(self, pod: str) -> F.HubInstanceObservation:
        self.observed.append(pod)
        if self.observe_error is not None:
            raise self.observe_error
        return self.observation


class LocalCredFleet(FakeFleet):
    """叠加 `LocalHubCredentialSource`(对应 mode=local)。"""

    def __init__(self, cands=None, *, cred=None) -> None:
        super().__init__(cands)
        self.cred = cred

    def local_credential_ack(self, pod: str):
        if pod != POD:
            return None
        return self.cred


@dataclasses.dataclass(slots=True)
class LocalCred:
    instance_uid: str = "local-uid"
    protocol_epoch: int = 11
    gen: int = 12
    jti: str = "local-jti"
    writer_epoch: int = DS_AUTH_WRITER_EPOCH_V2


class FakeSigner:
    def __init__(self, *, token: str = "tok", exp_ms: int = 999, error=None) -> None:
        self.token = token
        self.exp_ms = exp_ms
        self.error = error
        self.calls: list = []

    async def sign_hub_ticket(self, player_id: int, role_id: int, binding):
        self.calls.append((player_id, role_id, binding))
        if self.error is not None:
            raise self.error
        return self.token, self.exp_ms


class FakeOwnerAuth:
    """最小 owner 权威:Begin 恒回一份 exact 记录(除非脚本化成别的)。"""

    def __init__(self, *, begin_error=None, non_exact: bool = False) -> None:
        self.begin_error = begin_error
        self.non_exact = non_exact
        self.begins: list[OwnerTargetView] = []

    async def query_owner(self, player_id: int) -> OwnerRecordView:
        return OwnerRecordView(owner_epoch=1)

    async def begin_transition(self, player_id, expected_epoch, op_id, owner_type, target):
        # hub 侧只能写 HUB 归属;写成 BATTLE 会让 §9.22 的 exact 校验在别处才炸。
        assert owner_type == OWNER_TYPE_HUB
        self.begins.append(target)
        if self.begin_error is not None:
            raise self.begin_error
        pod = "somewhere-else" if self.non_exact else target.pod_name
        return OwnerRecordView(
            owner_epoch=expected_epoch + 1,
            owner_type=owner_type,
            phase=1,
            pod_name=pod,
            instance_uid=target.instance_uid,
            instance_epoch=target.instance_epoch,
            assignment_or_allocation_id=target.assignment_or_allocation_id,
            release_track=target.release_track,
            operation_id="op",
        )

    async def admit(self, *a, **kw):  # pragma: no cover —— 本批不走 Admit
        raise AssertionError("admit 不应被签票路径调用")


class Harness(bs.ShardMixin, HubUsecaseBase):
    """`HubUsecase` 的本批切片:ShardMixin + 基座 + 另一批提供的成员索引方法。

    `add_shard_member` / `remove_shard_member` 由后续批次提供,这里打桩并记账 ——
    它们是 saga 的**可观测副作用**,漏调用会让强制整合枚举不到玩家。
    """

    def __init__(self, repo, fleet, signer, cfg) -> None:
        super().__init__(repo, fleet, signer, cfg)
        self.added: list[tuple[str, int]] = []
        self.member_removed: list[tuple[str, int]] = []

    async def add_shard_member(self, pod: str, player_id: int) -> None:
        self.added.append((pod, player_id))

    async def remove_shard_member(self, pod: str, player_id: int) -> None:
        self.member_removed.append((pod, player_id))


def make_cfg(**kw) -> hconf.HubConf:
    base = {
        "heartbeat_timeout": "30s",
        "shard_ttl": "30m",
        "assignment_ttl": "30m",
        "reservation_ttl": "3m",
        "default_region": "global",
        "default_capacity": 500,
        "optimistic_retry": 3,
        "transfer_cooldown": "10s",
    }
    base.update(kw)
    return hconf.HubConf(**base)


def make_uc(
    *,
    repo: FakeRepo | None = None,
    fleet=None,
    signer=None,
    cfg: hconf.HubConf | None = None,
    auth_repo=None,
    owner_auth=None,
    require_heartbeat_ready: bool = False,
    ds_token_generation: bool = False,
) -> Harness:
    uc = Harness(repo or FakeRepo(), fleet or FakeFleet(), signer, cfg or make_cfg())
    uc.auth_repo = auth_repo
    uc.owner_auth = owner_auth
    uc.require_heartbeat_ready = require_heartbeat_ready
    uc.ds_token_generation = ds_token_generation
    return uc


# ═══════════════════════════════════════════════════════════════════════════
# 1. release track:唯一的旧值迁移规则
# ═══════════════════════════════════════════════════════════════════════════


async def test_sticky_release_track_maps_empty_and_rejects_unknown_with_errcode() -> None:
    """空轨迁移为 stable;未知轨 fail-closed 且**必须带 ErrInvalidState 码**。

    码值不是装饰:它一路传到 RPC 边界决定客户端"重试"还是"当参数错"。
    `fleet.sticky_release_track` 抛的是裸 `ValueError`,直接透出去客户端只会看到
    `UNKNOWN`,§9.23 要求的"每次等待有明确原因"当场打穿。

    ★ 变异:把 `sticky_release_track` 的 `except ValueError` 整段删掉(直接
      `return F.sticky_release_track(track)`)→ 本用例的 code 断言变红。
    """
    assert bs.sticky_release_track("") == releasetrack.STABLE
    assert bs.sticky_release_track(releasetrack.CANARY) == releasetrack.CANARY
    with pytest.raises(errcode.PandoraError) as ei:
        bs.sticky_release_track("prod")
    assert ei.value.code == errcode.ErrInvalidState
    assert "prod" in ei.value.msg


async def test_sticky_release_track_or_none_does_not_raise() -> None:
    """不抛版:一条脏分片只该被**跳过**,不该让整轮分配失败。

    ★ 变异:把 `sticky_release_track_or_none` 改成直接调 `bs.sticky_release_track`
      → 本用例(以及所有 census / least_loaded 的脏数据用例)变红。
    """
    assert bs.sticky_release_track_or_none("") == releasetrack.STABLE
    assert bs.sticky_release_track_or_none("prod") is None


# ═══════════════════════════════════════════════════════════════════════════
# 2. 身份判定:writer-v2 绑定完整性 / 同实例
# ═══════════════════════════════════════════════════════════════════════════


async def test_binding_v2_complete_accepts_full_record() -> None:
    """完整 writer-v2 绑定放行(基线,给下面的 knockout 做对照)。

    ★ 变异:把 `assignment_binding_v2_complete` 的 `return` 改成 `return False`
      → 本用例变红。
    """
    assert bs.assignment_binding_v2_complete(assignment(), PLAYER) is True


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("hub_pod_name", ""),
        ("hub_instance_uid", ""),
        ("auth_epoch", 0),
        ("auth_gen", 0),
        ("auth_jti", ""),
        ("assignment_id", ""),
        ("auth_writer_epoch", 1),  # legacy writer
        ("auth_writer_epoch", 3),  # future writer(同样必须拒:不许请求路径自升级)
        ("release_track", "prod"),  # 非法持久化轨
        ("player_id", PLAYER + 1),  # 记录属于别的玩家
    ],
)
async def test_binding_v2_complete_rejects_each_missing_piece(field: str, bad) -> None:
    """逐字段 knockout:少任何一格都不是完整绑定。

    这批断言防的是"半截绑定自己给自己发通行证" —— Model B 数据面永久只接受
    `writer_epoch == 2` 的完整 tuple,legacy / future 的迁移必须由 activation
    控制面在开放业务流量**之前**完成。

    ★ 变异:把 `assignment_binding_v2_complete` 里对应那一格的判据删掉
      (例如去掉 `and a.auth_jti != ""`)→ 对应参数化用例变红。
    """
    a = assignment()
    setattr(a, field, bad)
    assert bs.assignment_binding_v2_complete(a, PLAYER) is False


async def test_binding_v2_complete_rejects_none_and_zero_player() -> None:
    """None 记录 / player_id=0 一律 False(Go 里 nil receiver 也是这个结论)。

    ★ 变异:把首行 `if a is None: return False` 删掉 → 本用例抛 AttributeError 变红。
    """
    assert bs.assignment_binding_v2_complete(None, PLAYER) is False
    assert bs.assignment_binding_v2_complete(assignment(player_id=0), 0) is False


async def test_assignment_same_instance_happy() -> None:
    """UID + epoch + 轨 + 双侧 writer_epoch 全等 → 可原地重绑。

    ★ 变异:把 `assignment_same_instance` 的 `return` 改成 `return False` → 变红。
    """
    assert bs.assignment_same_instance(assignment(), seat()) is True


@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(lambda s: setattr(s, "instance_uid", "uid-bbb"), id="uid_changed"),
        pytest.param(lambda s: setattr(s, "protocol_epoch", 8), id="epoch_changed"),
        pytest.param(lambda s: setattr(s, "ok", False), id="not_routable"),
        pytest.param(lambda s: setattr(s, "writer_epoch", 1), id="current_writer_legacy"),
        pytest.param(
            lambda s: setattr(s, "release_track", releasetrack.CANARY), id="track_changed"
        ),
    ],
)
async def test_assignment_same_instance_rejects_instance_drift(mutate) -> None:
    """实例漂移的五种形态都必须判成"**不是**同一个实例"。

    这是"同名 Pod 的凭据轮换"与"同名 GameServer 重建"的分界线。判松一格,
    一台被重建的 GameServer 就会"继承"上一条实例的全部在场玩家(§9.22 脑裂),
    而日志上什么都看不到。

    ★ 变异:删掉 `assignment_same_instance` 里对应的那一条 `and`
      (如 `and a.hub_instance_uid == current.instance_uid`)→ 对应用例变红。
    """
    s = seat()
    mutate(s)
    assert bs.assignment_same_instance(assignment(), s) is False


async def test_assignment_same_instance_rejects_none() -> None:
    """任一侧缺失 → False(不能拿零值身份当"相同")。

    ★ 变异:删掉 `if a is None or current is None: return False` → 本用例变红。
    """
    assert bs.assignment_same_instance(None, seat()) is False
    assert bs.assignment_same_instance(assignment(), None) is False


# ═══════════════════════════════════════════════════════════════════════════
# 3. 快照搬运:authoritative_shard / bind_assignment_auth / identity
# ═══════════════════════════════════════════════════════════════════════════


async def test_authoritative_shard_overwrites_from_seat_and_clones() -> None:
    """权威 seat 覆盖六格,且**必须是克隆**。

    原地改会污染同一轮里其它候选的判定 —— Python 的 proto message 是引用语义。

    ★ 变异:把 `out = R.clone(shard)` 改成 `out = shard` → 本用例的
      "原分片未被修改"断言变红。
    """
    src = shard(count=1, cap=2, addr="old:1", region="cn", shard_id=9)
    out = bs.authoritative_shard(src, seat(addr="new:2", region="global", shard_id=3, count=4, cap=10))
    assert (out.hub_addr, out.region, out.shard_id) == ("new:2", "global", 3)
    assert (out.player_count, out.capacity, out.release_track) == (4, 10, releasetrack.STABLE)
    # 原对象一个字节都不能变。
    assert (src.hub_addr, src.region, src.shard_id, src.player_count) == ("old:1", "cn", 9, 1)


async def test_authoritative_shard_without_seat_is_plain_clone() -> None:
    """legacy(seat=None)只克隆,不编造权威值。

    ★ 变异:把 `if seat is not None:` 去掉 → 本用例抛 AttributeError 变红。
    """
    src = shard(count=1, addr="old:1")
    out = bs.authoritative_shard(src, None)
    assert out.hub_addr == "old:1"
    assert out is not src


async def test_bind_assignment_auth_is_noop_without_seat() -> None:
    """legacy 面不得伪造绑定 —— 伪造出来的身份指向虚无,却能骗过完整性检查。

    ★ 变异:把 `if seat is None: return` 删掉 → 本用例抛 AttributeError 变红;
      改成把零值写进去(`a.hub_instance_uid = ""` 等)→ 断言 uid 仍为原值变红。
    """
    a = assignment(uid="", epoch=0, gen=0, jti="", writer_epoch=0)
    bs.bind_assignment_auth(a, None)
    assert (a.hub_instance_uid, a.auth_epoch, a.auth_writer_epoch) == ("", 0, 0)

    bs.bind_assignment_auth(a, seat(uid="u2", epoch=9, gen=8, jti="j2"))
    assert (a.hub_instance_uid, a.auth_epoch, a.auth_gen, a.auth_jti) == ("u2", 9, 8, "j2")
    assert a.auth_writer_epoch == DS_AUTH_WRITER_EPOCH_V2


async def test_assignment_instance_identity_maps_exact_tuple() -> None:
    """退座身份取自归属记录的 exact 四元组;None → 零值(而不是崩)。

    ★ 变异:把 `instance_uid=a.hub_instance_uid` 写成 `instance_uid=""` → 变红。
    """
    ident = bs.assignment_instance_identity(assignment())
    assert ident == L.AssignmentInstanceIdentity(
        player_id=PLAYER,
        assignment_id=AID,
        instance_uid=UID,
        protocol_epoch=7,
        writer_epoch=DS_AUTH_WRITER_EPOCH_V2,
    )
    assert bs.assignment_instance_identity(None) == L.AssignmentInstanceIdentity()


# ═══════════════════════════════════════════════════════════════════════════
# 4. transfer cleanup 的字段搬运
# ═══════════════════════════════════════════════════════════════════════════


async def test_bind_transfer_cleanup_source_writes_all_phase_fields() -> None:
    """happy:八格全写,且 `release_cleanup_pending` 必须被压回 False(两相位互斥)。

    ★ 变异:把 `target.release_cleanup_pending = False` 删掉 → 本用例变红
      (随后 `transfer_cleanup_source` 会因"两相位并存"整条拒绝)。
    """
    target = assignment(aid="new-aid", pod="hub-2", uid="uid-bbb")
    target.release_cleanup_pending = True
    source = assignment(aid="old-aid", pod="hub-1", uid="uid-aaa", epoch=5)
    bs.bind_transfer_cleanup_source(target, source)
    assert target.transfer_cleanup_pending is True
    assert target.transfer_target_bound is False
    assert target.transfer_source_hub_pod_name == "hub-1"
    assert target.transfer_source_assignment_id == "old-aid"
    assert target.transfer_source_instance_uid == "uid-aaa"
    assert target.transfer_source_auth_epoch == 5
    assert target.transfer_source_auth_writer_epoch == DS_AUTH_WRITER_EPOCH_V2
    assert target.release_cleanup_pending is False


@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(lambda t, s: setattr(s, "assignment_id", t.assignment_id), id="same_aid"),
        pytest.param(lambda t, s: setattr(s, "player_id", PLAYER + 1), id="player_mismatch"),
        pytest.param(lambda t, s: setattr(s, "hub_pod_name", ""), id="no_source_pod"),
        pytest.param(lambda t, s: setattr(s, "hub_instance_uid", ""), id="no_source_uid"),
        pytest.param(lambda t, s: setattr(s, "auth_epoch", 0), id="no_source_epoch"),
        pytest.param(lambda t, s: setattr(s, "auth_writer_epoch", 1), id="legacy_writer"),
        pytest.param(lambda t, s: setattr(t, "assignment_id", ""), id="no_target_aid"),
        pytest.param(lambda t, s: setattr(t, "player_id", 0), id="zero_player"),
    ],
)
async def test_bind_transfer_cleanup_source_rejects_incomplete_identity(mutate) -> None:
    """八种"清不掉的残留"形态。

    `same_aid` 最凶:它会让清理去退**刚提交的新 owner** 自己的座位,
    玩家进场成功后立刻被踢。

    ★ 变异:删掉 `bind_transfer_cleanup_source` 里对应的那条 `or` 判据 →
      对应参数化用例变红(且 target 会被写上一份自毁的 cleanup 阶段)。
    """
    target = assignment(aid="new-aid", pod="hub-2")
    source = assignment(aid="old-aid", pod="hub-1")
    mutate(target, source)
    with pytest.raises(errcode.PandoraError) as ei:
        bs.bind_transfer_cleanup_source(target, source)
    assert ei.value.code == errcode.ErrInvalidState


async def test_transfer_cleanup_source_roundtrip() -> None:
    """bind → source 往返:重建出的记录只含 exact 身份五格。

    ★ 变异:把 `hub_pod_name=a.transfer_source_hub_pod_name` 写成
      `hub_pod_name=a.hub_pod_name` → 本用例变红(而线上会去退**目标**分片的座位)。
    """
    target = assignment(aid="new-aid", pod="hub-2", uid="uid-bbb")
    source = assignment(aid="old-aid", pod="hub-1", uid="uid-aaa", epoch=5)
    bs.bind_transfer_cleanup_source(target, source)
    rebuilt = bs.transfer_cleanup_source(target)
    assert rebuilt.player_id == PLAYER
    assert rebuilt.hub_pod_name == "hub-1"
    assert rebuilt.assignment_id == "old-aid"
    assert rebuilt.hub_instance_uid == "uid-aaa"
    assert rebuilt.auth_epoch == 5
    assert rebuilt.auth_writer_epoch == DS_AUTH_WRITER_EPOCH_V2


@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(lambda a: setattr(a, "transfer_cleanup_pending", False), id="not_pending"),
        pytest.param(lambda a: setattr(a, "release_cleanup_pending", True), id="both_phases"),
        pytest.param(
            lambda a: setattr(a, "transfer_source_assignment_id", a.assignment_id),
            id="source_is_self",
        ),
        pytest.param(lambda a: setattr(a, "transfer_source_instance_uid", ""), id="no_uid"),
        pytest.param(lambda a: setattr(a, "transfer_source_auth_epoch", 0), id="no_epoch"),
        pytest.param(
            lambda a: setattr(a, "transfer_source_auth_writer_epoch", 1), id="legacy_writer"
        ),
    ],
)
async def test_transfer_cleanup_source_rejects_broken_phase(mutate) -> None:
    """相位字段自相矛盾时 fail-closed(不许猜)。

    ★ 变异:删掉 `transfer_cleanup_source` 里对应的 `or` 判据 → 对应用例变红。
    """
    target = assignment(aid="new-aid", pod="hub-2")
    bs.bind_transfer_cleanup_source(target, assignment(aid="old-aid", pod="hub-1"))
    mutate(target)
    with pytest.raises(errcode.PandoraError) as ei:
        bs.transfer_cleanup_source(target)
    assert ei.value.code == errcode.ErrInvalidState


async def test_clear_transfer_cleanup_zeroes_every_field() -> None:
    """七格必须**全清**:留一格非零就会被 orphan 判据抓住,该玩家从此进不去场景。

    ★ 变异:删掉 `a.transfer_source_auth_epoch = 0` 这一行 → 本用例变红。
    """
    target = assignment(aid="new-aid", pod="hub-2")
    bs.bind_transfer_cleanup_source(target, assignment(aid="old-aid", pod="hub-1"))
    target.transfer_target_bound = True
    bs.clear_transfer_cleanup(target)
    assert target.transfer_cleanup_pending is False
    assert target.transfer_target_bound is False
    assert target.transfer_source_hub_pod_name == ""
    assert target.transfer_source_assignment_id == ""
    assert target.transfer_source_instance_uid == ""
    assert target.transfer_source_auth_epoch == 0
    assert target.transfer_source_auth_writer_epoch == 0


async def test_transfer_cleanup_ref_of_none_is_invalid() -> None:
    """None → 零值 ref(`valid()` 为假),不会往索引里塞一条清不掉的垃圾。

    ★ 变异:去掉 `if a is None` 分支 → 本用例抛 AttributeError 变红。
    """
    assert bs.transfer_cleanup_ref(None).valid() is False
    ref = bs.transfer_cleanup_ref(assignment())
    assert (ref.player_id, ref.target_assignment_id) == (PLAYER, AID)


# ═══════════════════════════════════════════════════════════════════════════
# 5. 票据绑定 / 交付等价
# ═══════════════════════════════════════════════════════════════════════════


async def test_ticket_binding_from_assignment_happy() -> None:
    """七元组齐备时逐格搬运。

    ★ 变异:把 `credential_gen=a.auth_gen` 写成 `credential_gen=0` → 变红
      (线上表现为 UE 侧 ACK 比对失败、玩家 7s 重连循环)。
    """
    b = bs.ticket_binding_from_assignment(assignment())
    assert b.pod_name == POD
    assert b.instance_uid == UID
    assert b.protocol_epoch == 7
    assert b.credential_gen == 3
    assert b.credential_jti == "jti-aaa"
    assert b.hub_assignment_id == AID
    assert b.writer_epoch == DS_AUTH_WRITER_EPOCH_V2
    assert b.release_track == releasetrack.STABLE


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("hub_pod_name", ""),
        ("hub_instance_uid", ""),
        ("auth_epoch", 0),
        ("auth_gen", 0),
        ("auth_jti", ""),
        ("assignment_id", ""),
        ("auth_writer_epoch", 1),
        ("release_track", "prod"),
    ],
)
async def test_ticket_binding_returns_zero_on_any_missing_piece(field: str, bad) -> None:
    """缺任一格必须返回**零值绑定**,不许交半截。

    半截绑定签出的票在 UE Hub DS PostLogin 会 fail-closed 踢人,客户端只看到
    "连上又被踢",排查方向完全错。

    ★ 变异:删掉 `ticket_binding_from_assignment` 里对应的判据 → 对应用例变红。
    """
    a = assignment()
    setattr(a, field, bad)
    assert bs.ticket_binding_from_assignment(a).pod_name == ""


async def test_ticket_binding_of_none_is_zero() -> None:
    """None → 零值绑定(Go 那侧 nil receiver 也是这个结论)。

    ★ 变异:删掉 `if a is None: return HubTicketBinding()` → AttributeError 变红。
    """
    assert bs.ticket_binding_from_assignment(None).pod_name == ""


async def test_delivery_equal_happy_and_ignores_cleanup_phase() -> None:
    """交付等价**刻意忽略** cleanup 相位与时间戳。

    那些字段会在 cleanup 推进时合法变化;把它们算进来,一次正常的 Transfer 会
    永远交付不出票(guard 每次都判"归属被别人抢走了")。

    ★ 变异:在 `hub_assignment_delivery_equal` 里补一条
      `and a.transfer_target_bound == b.transfer_target_bound` → 本用例变红。
    """
    a, b = assignment(), assignment()
    b.transfer_target_bound = True
    b.assigned_at_ms = 12345
    assert bs.hub_assignment_delivery_equal(a, b, PLAYER) is True


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("assignment_id", "other"),
        ("hub_pod_name", "hub-9"),
        ("hub_addr", "9.9.9.9:1"),
        ("shard_id", 77),
        ("region", "cn"),
        ("team_id", 999),
        ("role_id", 66),
        ("hub_instance_uid", "uid-zzz"),
        ("auth_epoch", 99),
        ("auth_gen", 99),
        ("auth_jti", "jti-zzz"),
        ("auth_writer_epoch", 1),
        ("release_track", releasetrack.CANARY),
    ],
)
async def test_delivery_equal_rejects_any_routing_relevant_drift(field: str, bad) -> None:
    """任何会改变**路由 / 角色 / 票据绑定**的字段变化都必须判不等。

    这 13 格是 `bind_owner_for_published_hub_assignment` 的 guard 唯一依据:
    漏一格就等于允许把票交给一个已经被后继 assignment 取代的目标。

    ★ 变异:删掉 `hub_assignment_delivery_equal` 里对应的那条 `and` → 对应用例变红。
    """
    a, b = assignment(), assignment()
    setattr(b, field, bad)
    assert bs.hub_assignment_delivery_equal(a, b, PLAYER) is False


async def test_delivery_equal_requires_both_sides_belong_to_player() -> None:
    """两侧都必须属于本玩家;任一侧是 None 也判不等。

    ★ 变异:去掉 `or b.player_id != player_id` → 第一个断言变红。
    """
    a = assignment()
    b = assignment(player_id=PLAYER + 1)
    assert bs.hub_assignment_delivery_equal(a, b, PLAYER) is False
    assert bs.hub_assignment_delivery_equal(a, None, PLAYER) is False
    assert bs.hub_assignment_delivery_equal(None, a, PLAYER) is False


async def test_owner_target_view_equal_is_field_wise_including_source_revision() -> None:
    """OwnerTargetView 的 `==` 必须逐字段(含 source_revision)。

    guard 里 `ownerTargetViewEqual` 就是靠它发现"目标已经不是刚才那个了"。

    ★ 变异:把 `owner_target_view_equal` 改成 `return a.pod_name == b.pod_name`
      → 后两个断言变红。
    """
    t = target_of(assignment())
    assert bs.owner_target_view_equal(t, target_of(assignment())) is True
    assert bs.owner_target_view_equal(t, dataclasses.replace(t, instance_uid="x")) is False
    assert bs.owner_target_view_equal(t, dataclasses.replace(t, source_revision=1)) is False


# ═══════════════════════════════════════════════════════════════════════════
# 6. 选分片:least_loaded / select_transfer_target
# ═══════════════════════════════════════════════════════════════════════════


async def test_least_loaded_filters_and_breaks_ties_by_shard_id() -> None:
    """五道筛(轨 / region / ready / 未满 / 排除 pod)+ shard_id tie-break。

    tie-break 必须是 `shard_id` 而不是遍历顺序:`list_shards` 的顺序来自 Redis SET,
    两次调用可能不同,不定序会让同一批队友被打散到不同分片。

    ★ 变异:把 tie-break 条件 `and s.shard_id < best.shard_id` 删掉 → 本用例变红。
    """
    shards = [
        shard("a", shard_id=5, count=1),
        shard("b", shard_id=2, count=1),  # 与 a 同人数,shard_id 更小 → 胜出
        shard("c", shard_id=1, count=0, track=releasetrack.CANARY),  # 轨不同
        shard("d", shard_id=1, count=0, region="cn"),  # region 不同
        shard("e", shard_id=1, count=0, state=STATE_WARMING),  # 未 ready
        shard("f", shard_id=1, count=10, cap=10),  # 满
        shard("g", shard_id=1, count=0, track="prod"),  # 持久化轨非法
    ]
    best = bs.least_loaded(shards, "global", releasetrack.STABLE, "")
    assert best is not None
    assert best.hub_pod_name == "b"


async def test_least_loaded_honours_exclude_pod() -> None:
    """排除当前分片后仍能选到次优;全被排除则 None。

    ★ 变异:把 `if exclude_pod != "" and s.hub_pod_name == exclude_pod: continue`
      删掉 → 第一个断言变红(会选回被排除的那台)。
    """
    shards = [shard("a", shard_id=1, count=0), shard("b", shard_id=2, count=5)]
    assert bs.least_loaded(shards, "global", releasetrack.STABLE, "a").hub_pod_name == "b"
    assert bs.least_loaded([shards[0]], "global", releasetrack.STABLE, "a") is None


async def test_select_transfer_target_rejects_out_of_uint32_hub_id() -> None:
    """越界 `target_hub_id` 必须**不匹配**,不能靠低位截断误中。

    Go 那侧 `uint32(targetHubID)` 会静默截断,`0x1_0000_0001` 会误匹配 shard_id=1。
    Python 的 int 不截断 —— 若不显式拒绝,两栈对同一入参会给出不同结论。

    ★ 变异:删掉 `if target_hub_id > _UINT32_MAX: return None` → 本用例仍绿
      (因为 Python 不截断),所以这里额外断言"截断语义没有被引入":
      把判据改成 `want = target_hub_id & _UINT32_MAX` 才会让本用例变红。
    """
    cur = shard("hub-1", shard_id=9, count=1)
    shards = [cur, shard("hub-2", shard_id=1, count=0)]
    assert bs.select_transfer_target(shards, cur, (1 << 32) + 1) is None
    # 对照:低位相同的合法值确实能命中,证明上面的 None 来自越界判据而非拼错。
    assert bs.select_transfer_target(shards, cur, 1).hub_pod_name == "hub-2"


async def test_select_transfer_target_named_target_capacity_rule() -> None:
    """点名目标 = 当前分片时不要求"未满"(幂等重签不占新座位);点名别处则必须未满。

    ★ 变异:把 `if s.hub_pod_name == cur.hub_pod_name or s.player_count < s.capacity`
      改成只保留 `s.player_count < s.capacity` → 第一个断言变红(自愈重签会失败)。
    """
    cur = shard("hub-1", shard_id=9, count=10, cap=10)  # 自己已满
    other_full = shard("hub-2", shard_id=1, count=10, cap=10)
    shards = [cur, other_full]
    assert bs.select_transfer_target(shards, cur, 9) is cur
    assert bs.select_transfer_target(shards, cur, 1) is None


async def test_select_transfer_target_keeps_release_track_sticky() -> None:
    """切线不得换轨(§9.21 玩家轨道粘性);当前记录轨非法则整条拒绝。

    ★ 变异:删掉 `and track == cur_track` → 第一个断言变红(canary 玩家被切回 stable)。
    """
    cur = shard("hub-1", shard_id=9, count=1, track=releasetrack.CANARY)
    stable_free = shard("hub-2", shard_id=1, count=0, track=releasetrack.STABLE)
    assert bs.select_transfer_target([cur, stable_free], cur, 1) is None
    assert bs.select_transfer_target([cur, stable_free], cur, 0) is None
    bad = shard("hub-3", shard_id=9, count=1, track="prod")
    assert bs.select_transfer_target([bad, stable_free], bad, 0) is None


async def test_select_transfer_target_fallback_excludes_current_pod() -> None:
    """不点名时回落 `least_loaded` 并排除当前分片(否则"切线"会切到原地)。

    ★ 变异:把最后一行的 `cur.hub_pod_name` 换成 `""` → 本用例变红。
    """
    cur = shard("hub-1", shard_id=9, count=0)
    other = shard("hub-2", shard_id=1, count=5)
    assert bs.select_transfer_target([cur, other], cur, 0).hub_pod_name == "hub-2"


# ═══════════════════════════════════════════════════════════════════════════
# 7. census:为什么没 hub
# ═══════════════════════════════════════════════════════════════════════════


async def test_census_sample_is_capped_and_formatted_like_go() -> None:
    """样本上限 12,格式逐字符对齐 Go 的 `fmt.Sprintf`(计数**不**截断)。

    ★ 变异:把 `SHARD_CENSUS_SAMPLE_LIMIT` 改成 100 → 长度断言变红;
      把分隔符 `|` 换成 `,` → 格式断言变红。
    """
    c = bs.ShardExclusionCensus()
    for i in range(20):
        c.observe(shard(f"p{i}", shard_id=i, count=i, cap=9), "candidate")
    assert len(c.sample) == 12
    assert c.sample[0] == "p0|ready|stable|shard=0|0/9|candidate"


async def test_census_fields_key_names_are_frozen() -> None:
    """字段名是 Loki / Grafana 面板列名,改一个字母面板只统计到一半且**无告警**。

    ★ 变异:把 `"excl_not_ready"` 改成 `"excl_notready"` → 本用例变红。
    """
    assert set(bs.ShardExclusionCensus().fields()) == {
        "shards_total",
        "candidates",
        "excl_track_invalid",
        "excl_track_mismatch",
        "excl_region_mismatch",
        "excl_pod",
        "excl_not_ready",
        "excl_warming",
        "excl_draining",
        "excl_stopping",
        "excl_full",
        "reserve_rejected",
        "shard_census",
    }


@pytest.mark.parametrize(
    ("census", "want"),
    [
        # 顺序即处置优先级,逐条构造成"前面的分支都不成立"。
        (bs.ShardExclusionCensus(), "no_shard_mirror"),
        (
            bs.ShardExclusionCensus(total=3, candidates=2, reserve_rejected=2),
            "all_candidates_reserve_rejected",
        ),
        (bs.ShardExclusionCensus(total=3, candidates=2), "candidates_vanished"),
        (bs.ShardExclusionCensus(total=3, full=2, not_ready=1), "all_shards_full"),
        # full < not_ready 时不算"满了" —— 大多数分片其实是没心跳。
        (bs.ShardExclusionCensus(total=3, full=1, not_ready=2, warming=2), "all_shards_warming"),
        (bs.ShardExclusionCensus(total=3, not_ready=1, draining=1), "all_shards_draining"),
        (bs.ShardExclusionCensus(total=3, not_ready=1, stopping=1), "all_shards_draining"),
        (bs.ShardExclusionCensus(total=3, region_mismatch=3), "no_shard_in_region"),
        # region_mismatch 与 track_mismatch 并存时优先报轨(灰度调参更可能是原因)。
        (
            bs.ShardExclusionCensus(total=3, region_mismatch=1, track_mismatch=2),
            "no_shard_in_release_track",
        ),
        (bs.ShardExclusionCensus(total=3, track_invalid=3), "all_shards_track_invalid"),
        (bs.ShardExclusionCensus(total=3, excluded_pod=3), "no_shard_candidate"),
    ],
)
async def test_no_routable_shard_reason_branch_order(census, want: str) -> None:
    """十一种成因各自收敛到唯一 reason,且**顺序**不能重排。

    玩家侧只会看到一句"没有可用 hub";到底是真满了、全在 warming、还是 region
    传错了,这四种的处置方案完全不同,只能靠这里的分支区分。

    ★ 变异:把 `if c.warming > 0` 挪到 `if c.full > 0 ...` 之前 →
      `all_shards_full` 那条变红。
    """
    assert bs.no_routable_shard_reason(census) == want


# ═══════════════════════════════════════════════════════════════════════════
# 8. ensure_shards / reconcile_shard_topology
# ═══════════════════════════════════════════════════════════════════════════


async def test_ensure_shards_rejects_invalid_track_before_touching_fleet() -> None:
    """非法轨在打 Fleet **之前**就拒(fail-closed,且不浪费一次 apiserver 调用)。

    ★ 变异:把 `if not releasetrack.valid(release_track)` 删掉 → 本用例变红。
    """
    fleet = FakeFleet([candidate()])
    uc = make_uc(fleet=fleet)
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.ensure_shards("global", "prod")
    assert ei.value.code == errcode.ErrInvalidArg
    assert fleet.calls == []


async def test_ensure_shards_is_lazy_when_region_track_already_present() -> None:
    """该 region + 轨已有分片就直接返回,**不打 Fleet**。

    每次登录都查 apiserver 会让 AssignHub 的延迟随集群规模劣化,而拓扑漂移不紧急
    (交后台对账)。

    ★ 变异:把命中分支的 `return` 删掉 → `fleet.calls` 断言变红。
    """
    repo = FakeRepo([shard()])
    fleet = FakeFleet([candidate()])
    uc = make_uc(repo=repo, fleet=fleet)
    await uc.ensure_shards("global", releasetrack.STABLE)
    assert fleet.calls == []
    assert repo.created == []


async def test_ensure_shards_skips_unusable_candidates_and_seeds_the_rest() -> None:
    """令牌未就绪 / 轨非法的候选**不种镜像**;其余按种子字段落库。

    种一条"看起来 ready、实际回调全 401"的镜像 = 玩家被路由过去后连得上、进不去。

    ★ 变异:把 `if not c.token_ready or not releasetrack.valid(c.release_track): continue`
      删掉 → 本用例的"只种了一条"断言变红。
    """
    repo = FakeRepo([])
    fleet = FakeFleet(
        [
            candidate("bad-token", shard_id=1, token_ready=False),
            candidate("bad-track", shard_id=2, track="prod"),
            candidate("good", shard_id=3, uid="uid-x", epoch=5),
        ]
    )
    uc = make_uc(repo=repo, fleet=fleet, require_heartbeat_ready=True)
    await uc.ensure_shards("global", releasetrack.STABLE)
    assert [r.hub_pod_name for r in repo.created] == ["good"]
    rec = repo.created[0]
    assert rec.state == STATE_WARMING  # require_heartbeat_ready → 先 warming
    assert rec.last_heartbeat_ms == 0
    assert rec.player_count == 0
    assert (rec.gameserver_uid, rec.auth_epoch) == ("uid-x", 5)


async def test_reconcile_keeps_mirror_when_fleet_unavailable() -> None:
    """Fleet 不可用时**保留现有镜像**:列举缺席永远不等于进程已死(§9.22)。

    ★ 变异:把 `except BaseException` 那支的 `continue` 改成 `raise` → 本用例变红;
      若把"缺席即删镜像"写进来,`repo.shards` 断言变红。
    """
    repo = FakeRepo([shard()])
    fleet = FakeFleet(error=RuntimeError("apiserver down"))
    uc = make_uc(repo=repo, fleet=fleet)
    await uc.reconcile_shard_topology()
    assert [s.hub_pod_name for s in repo.shards] == [POD]
    assert repo.shards[0].state == STATE_READY


async def test_reconcile_marks_token_unready_candidate_warming_not_draining() -> None:
    """凭据失败只取消**路由资格**(→ warming),不动归属账本、不当排空。

    ★ 变异:把 `s.state = STATE_WARMING` 改成 `STATE_DRAINING` → 本用例变红
      (排空是不可逆的缩容语义,会把一台只是令牌轮换中的 DS 判死)。
    """
    repo = FakeRepo([shard(state=STATE_READY)])
    fleet = FakeFleet([candidate(token_ready=False)])
    uc = make_uc(repo=repo, fleet=fleet)
    await uc.reconcile_shard_topology()
    assert repo.shards[0].state == STATE_WARMING


async def test_reconcile_drains_shard_whose_track_drifted() -> None:
    """metadata 轨漂移 → 立刻退出可分配集,且**保留原轨证据**(不覆盖成新轨)。

    ★ 变异:把 `s.state = STATE_DRAINING; return` 里的 `return` 删掉 →
      "原轨仍是 stable"断言变红(记录被改写成 canary,漂移证据丢失)。
    """
    repo = FakeRepo([shard(track=releasetrack.STABLE, addr="old:1")])
    fleet = FakeFleet([candidate(track=releasetrack.CANARY)])
    uc = make_uc(repo=repo, fleet=fleet)
    await uc.reconcile_shard_topology()
    assert repo.shards[0].state == STATE_DRAINING
    assert repo.shards[0].release_track == releasetrack.STABLE
    assert repo.shards[0].hub_addr == "old:1"


async def test_reconcile_demotes_never_heartbeated_ready_shard() -> None:
    """滚动升级投毒防护:旧镜像建的 `ready + last_heartbeat_ms=0` 必须降回 warming。

    不降的话,新镜像开启心跳门控后,这台**从未发过鉴权心跳**的分片会被直接选中。

    ★ 变异:删掉 `if self.require_heartbeat_ready and s.last_heartbeat_ms == 0 ...` →
      本用例变红。
    """
    repo = FakeRepo([shard(state=STATE_READY, heartbeat_ms=0)])
    fleet = FakeFleet([candidate()])
    uc = make_uc(repo=repo, fleet=fleet, require_heartbeat_ready=True)
    await uc.reconcile_shard_topology()
    assert repo.shards[0].state == STATE_WARMING


async def test_reconcile_token_gen_only_advances() -> None:
    """令牌代际**只增不减**:低代际 / 0 候选不得清除既有代际(fail-open 向量)。

    ★ 变异:把 `if gen > s.current_token_gen` 改成 `if gen != s.current_token_gen`
      → 本用例的"代际仍为 9"断言变红。
    """
    repo = FakeRepo([shard(state=STATE_READY, token_gen=9, heartbeat_ms=1)])
    fleet = FakeFleet([candidate(token_gen=3)])
    uc = make_uc(repo=repo, fleet=fleet, require_heartbeat_ready=True, ds_token_generation=True)
    await uc.reconcile_shard_topology()
    assert repo.shards[0].current_token_gen == 9
    assert repo.shards[0].state == STATE_READY  # 未推进代际就不复位


async def test_reconcile_new_gen_resets_to_warming() -> None:
    """更高代际推进后复位 warming,等**新代际**的鉴权心跳 —— 挡旧令牌迟到心跳。

    ★ 变异:删掉推进分支里的 `s.state = STATE_WARMING` → 本用例变红。
    """
    repo = FakeRepo([shard(state=STATE_READY, token_gen=3, heartbeat_ms=1)])
    fleet = FakeFleet([candidate(token_gen=9, token_exp_ms=777)])
    uc = make_uc(repo=repo, fleet=fleet, require_heartbeat_ready=True, ds_token_generation=True)
    await uc.reconcile_shard_topology()
    assert repo.shards[0].current_token_gen == 9
    assert repo.shards[0].current_token_exp_ms == 777
    assert repo.shards[0].state == STATE_WARMING


async def test_reconcile_fences_absent_shard_but_needs_exact_proof_for_teardown() -> None:
    """候选里完全消失 → 标 draining(栅栏);拆机证明只能来自 exact UID 观测。

    "不可路由"与"已拆机"是两件事:把前者当后者就会在旧 DS 还持有玩家时开放第二台。

    ★ 变异:把 `if not observation.proves_teardown(...)` 那支的 `continue` 删掉 →
      "未铸 proof"断言变红(等于给任意残留发通行证)。
    """
    stale = shard("gone", uid="uid-gone")
    repo = FakeRepo([stale])
    auth = FakeAuthRepo()
    # GameServer 还在且 UID 正是期望的 → 显然没被拆。
    fleet = ObservingFleet(
        [], observation=F.HubInstanceObservation(game_server_found=True, game_server_uid="uid-gone")
    )
    uc = make_uc(repo=repo, fleet=fleet, auth_repo=auth)
    await uc.reconcile_shard_topology()
    assert repo.shards[0].state == STATE_DRAINING
    assert fleet.observed == ["gone"]
    assert auth.proofs == []


async def test_reconcile_records_teardown_proof_on_exact_uid_replacement() -> None:
    """exact UID 已被替换(GameServer 换 UID 且 Pod owner 指向新 UID)→ 铸拆机证明。

    ★ 变异:把 `record_instance_teardown_proof` 调用删掉 → 本用例变红。
    """
    stale = shard("gone", uid="uid-old")
    repo = FakeRepo([stale])
    auth = FakeAuthRepo()
    fleet = ObservingFleet(
        [],
        observation=F.HubInstanceObservation(
            game_server_found=True,
            game_server_uid="uid-new",
            pod_found=True,
            pod_owner_game_server_uid="uid-new",
        ),
    )
    uc = make_uc(repo=repo, fleet=fleet, auth_repo=auth)
    await uc.reconcile_shard_topology()
    assert auth.proofs == [("gone", "uid-old")]


async def test_reconcile_skips_teardown_when_fleet_cannot_observe() -> None:
    """fleet 不实现观测协议 → 只栅栏、不铸证明(Go 的类型断言失败那一支)。

    ★ 变异:把 `isinstance(self.fleet, F.HubFleetPhysicalObserver)` 改成 `True`
      → 本用例抛 AttributeError 变红。
    """
    repo = FakeRepo([shard("gone", uid="uid-old")])
    auth = FakeAuthRepo()
    uc = make_uc(repo=repo, fleet=FakeFleet([]), auth_repo=auth)
    await uc.reconcile_shard_topology()
    assert repo.shards[0].state == STATE_DRAINING
    assert auth.proofs == []


async def test_reconcile_does_not_revive_intentional_drain() -> None:
    """带 `draining_since_ms` 的**主动缩容排空**不可逆,不被栅栏 / 复位改回。

    ★ 变异:把 `not (current.state == STATE_DRAINING and current.draining_since_ms > 0)`
      简化成 `True` 也不会变红(它本来就是 draining);把 `_refresh` 里的
      `if s.state == STATE_DRAINING and s.draining_since_ms == 0` 判据里的
      `and s.draining_since_ms == 0` 删掉 → 本用例变红(排空中的分片被拉回 ready)。
    """
    repo = FakeRepo([shard(state=STATE_DRAINING, draining_since_ms=1234, heartbeat_ms=1)])
    fleet = FakeFleet([candidate()])
    uc = make_uc(repo=repo, fleet=fleet)
    await uc.reconcile_shard_topology()
    assert repo.shards[0].state == STATE_DRAINING
    assert repo.shards[0].draining_since_ms == 1234


# ═══════════════════════════════════════════════════════════════════════════
# 9. 占座 / 退座
# ═══════════════════════════════════════════════════════════════════════════


async def test_select_shard_prefers_team_shard_then_falls_back() -> None:
    """队友分片优先;队伍提示查询失败**静默降级**为"没有提示"。

    让一个软提示硬阻断分配 = 用体验优化把玩家挡在门外。

    ★ 变异:把 `except BaseException: pod = None` 改成 `raise` → 第二个断言变红。
    """
    shards = [shard("a", shard_id=1, count=0), shard("b", shard_id=2, count=5)]
    repo = FakeRepo(shards, team_shard="b")
    uc = make_uc(repo=repo)
    assert (await uc.select_shard("global", 42)).hub_pod_name == "b"

    repo.team_error = RuntimeError("redis down")
    assert (await uc.select_shard("global", 42)).hub_pod_name == "a"


async def test_select_shard_raises_when_nothing_available() -> None:
    """一个都选不出 → ErrHubNoAvailable(而不是返回 None 让调用方裸崩)。

    ★ 变异:把 `raise` 改成 `return None` → 本用例变红。
    """
    uc = make_uc(repo=FakeRepo([shard(state=STATE_WARMING)]))
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.select_shard("global", 0)
    assert ei.value.code == errcode.ErrHubNoAvailable


async def test_reserve_seat_rechecks_ready_and_capacity_inside_lock() -> None:
    """锁内复核 ready + 容量,通过才 `player_count++`。

    ★ 变异:把 `if s.player_count >= s.capacity` 改成 `>` → 满员用例变红
      (第 501 个玩家被放进 500 人上限的分片)。
    """
    ok = shard("ok", count=1, cap=2)
    repo = FakeRepo([ok, shard("warm", state=STATE_WARMING), shard("full", count=2, cap=2)])
    uc = make_uc(repo=repo)
    await uc.reserve_seat("ok")
    assert ok.player_count == 2
    for pod in ("warm", "full"):
        with pytest.raises(errcode.PandoraError) as ei:
            await uc.reserve_seat(pod)
        assert ei.value.code == errcode.ErrHubNoAvailable


async def test_release_from_shard_floors_at_zero_and_swallows_missing_shard() -> None:
    """退座 floor 0;分片已不存在时**静默**(补偿路径必须能重复跑)。

    把"分片已经没了"当失败会让调用方误以为座位还占着而反复重试。

    ★ 变异:把 `if s.player_count > 0` 删掉 → 计数变成 -1,第一个断言变红;
      把 `except BaseException` 整段删掉 → 第二个断言变红。
    """
    s = shard(count=0)
    uc = make_uc(repo=FakeRepo([s]))
    await uc.release_from_shard(POD)
    assert s.player_count == 0
    await uc.release_from_shard("nonexistent")  # 不抛


async def test_reserve_routable_seat_legacy_takes_plain_seat() -> None:
    """legacy(未装配 auth_repo)走纯容量占座并返回 None。

    ★ 变异:把 `if self.auth_repo is None` 分支删掉 → 本用例抛 AttributeError 变红。
    """
    s = shard(count=0, cap=2)
    uc = make_uc(repo=FakeRepo([s]))
    assert await uc.reserve_routable_seat(POD, PLAYER, AID) is None
    assert s.player_count == 1


async def test_reserve_routable_seat_fails_closed_when_not_routable() -> None:
    """`check_routable` 不 OK → ErrHubNoAvailable,且**不得**继续去占座。

    ★ 变异:把 `if not current.ok:` 那支的 `raise` 删掉 → "未调用 reserve"断言变红。
    """
    auth = FakeAuthRepo(routable=seat(ok=False, reason="auth-missing"))
    uc = make_uc(auth_repo=auth)
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.reserve_routable_seat(POD, PLAYER, AID)
    assert ei.value.code == errcode.ErrHubNoAvailable
    assert "auth-missing" in ei.value.msg
    assert auth.reserved == []


async def test_reserve_routable_seat_binds_active_tuple_from_routable_snapshot() -> None:
    """reservation 身份必须取自**同一次** `check_routable` 的 active 元组。

    取错来源(比如用归属记录里的旧 uid)会让 reservation 挂在一个已经不存在的实例上,
    随后被 prune 掉 —— 玩家占了个不存在的座位。

    ★ 变异:把 `instance_uid=current.instance_uid` 改成 `instance_uid=""` →
      本用例变红。
    """
    auth = FakeAuthRepo(routable=seat(uid="uid-live", epoch=42))
    uc = make_uc(auth_repo=auth)
    res = await uc.reserve_routable_seat(POD, PLAYER, AID)
    assert res is auth.reserve
    pod, ident = auth.reserved[0]
    assert pod == POD
    assert (ident.player_id, ident.assignment_id) == (PLAYER, AID)
    assert (ident.instance_uid, ident.protocol_epoch) == ("uid-live", 42)
    assert ident.writer_epoch == DS_AUTH_WRITER_EPOCH_V2
    # 绝对 lease:reservation 先到期,assignment 后到期(反了会让座位活得比归属久)。
    assert ident.expires_at_ms < ident.assignment_expires_at_ms


async def test_reserve_routable_seat_rejects_when_reserve_not_ok() -> None:
    """占座事务返回 not-OK(并发占满 / 元组不符)→ ErrHubNoAvailable。

    ★ 变异:把 `if not res.ok:` 删掉 → 本用例变红(返回一个 ok=False 的假座位)。
    """
    auth = FakeAuthRepo(reserve=seat(ok=False, reason="shard-full"))
    uc = make_uc(auth_repo=auth)
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.reserve_routable_seat(POD, PLAYER, AID)
    assert ei.value.code == errcode.ErrHubNoAvailable
    assert "shard-full" in ei.value.msg


async def test_ensure_existing_assignment_seat_requires_reusable_identity() -> None:
    """身份不可复用(实例已漂移 / 玩家不符 / 缺 assignment_id)→ ErrInvalidState。

    ★ 变异:把 `not assignment_same_instance(assignment, current)` 删掉 →
      第一个子用例变红(会拿一份属于**旧实例**的身份去续座)。
    """
    auth = FakeAuthRepo()
    uc = make_uc(auth_repo=auth)
    for a, cur in (
        (assignment(), seat(uid="uid-other")),
        (assignment(player_id=PLAYER + 1), seat()),
        (assignment(aid=""), seat()),
        (None, seat()),
        (assignment(), None),
    ):
        with pytest.raises(errcode.PandoraError) as ei:
            await uc.ensure_existing_assignment_seat(PLAYER, a, cur)
        assert ei.value.code == errcode.ErrInvalidState
    assert auth.reserved == []


async def test_ensure_existing_assignment_seat_legacy_passthrough() -> None:
    """legacy 面直接返回入参(没有账本可续)。

    ★ 变异:把 `if self.auth_repo is None: return current` 删掉 → 本用例变红。
    """
    uc = make_uc()
    cur = seat()
    assert await uc.ensure_existing_assignment_seat(PLAYER, assignment(), cur) is cur


async def test_ensure_existing_assignment_seat_raises_on_capacity_reject() -> None:
    """账本拒绝续座 → ErrHubNoAvailable(带 reason 便于定位)。

    ★ 变异:把 `if not res.ok:` 删掉 → 本用例变红。
    """
    auth = FakeAuthRepo(reserve=seat(ok=False, reason="successor-conflict"))
    uc = make_uc(auth_repo=auth)
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.ensure_existing_assignment_seat(PLAYER, assignment(), seat())
    assert ei.value.code == errcode.ErrHubNoAvailable
    assert "successor-conflict" in ei.value.msg


# ═══════════════════════════════════════════════════════════════════════════
# 10. assignment_routable
# ═══════════════════════════════════════════════════════════════════════════


async def test_assignment_routable_model_b_happy() -> None:
    """active 元组仍等于归属钉住的值 → 可路由,并回填归一化后的轨。

    ★ 变异:把最后的 `return info, True` 改成 `return info, False` → 变红。
    """
    auth = FakeAuthRepo(routable=seat(track=""))  # 空轨:考验归一化回填
    uc = make_uc(auth_repo=auth)
    info, ok = await uc.assignment_routable(PLAYER, assignment())
    assert ok is True
    assert info.release_track == releasetrack.STABLE


@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(lambda s: setattr(s, "instance_uid", "uid-zzz"), id="uid_drift"),
        pytest.param(lambda s: setattr(s, "protocol_epoch", 99), id="epoch_drift"),
        pytest.param(lambda s: setattr(s, "active_gen", 99), id="gen_drift"),
        pytest.param(lambda s: setattr(s, "active_jti", "jti-zzz"), id="jti_drift"),
        pytest.param(lambda s: setattr(s, "writer_epoch", 1), id="writer_drift"),
        pytest.param(lambda s: setattr(s, "ok", False), id="not_routable"),
        pytest.param(
            lambda s: setattr(s, "release_track", releasetrack.CANARY), id="track_drift"
        ),
    ],
)
async def test_assignment_routable_detects_every_drift(mutate) -> None:
    """七种漂移都必须判成"不可路由"(而不是"存储坏了")。

    区分很重要:不可路由是**可恢复**状态(客户端退避重查),抛异常会让 §9.23 的
    恢复链把它当终态错误处理。

    ★ 变异:删掉 `assignment_routable` 里对应的那条判据 → 对应用例变红。
    """
    s = seat()
    mutate(s)
    auth = FakeAuthRepo(routable=s)
    uc = make_uc(auth_repo=auth)
    _, ok = await uc.assignment_routable(PLAYER, assignment())
    assert ok is False


async def test_assignment_routable_rejects_incomplete_binding_under_model_b() -> None:
    """Model B 下不完整绑定是**数据不自洽**,抛 ErrInvalidState 而不是"不可路由"。

    ★ 变异:把 `if not assignment_binding_v2_complete(...)` 删掉 → 本用例变红。
    """
    uc = make_uc(auth_repo=FakeAuthRepo())
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.assignment_routable(PLAYER, assignment(writer_epoch=1))
    assert ei.value.code == errcode.ErrInvalidState


async def test_assignment_routable_raises_on_invalid_persisted_track() -> None:
    """归属记录持久化轨非法 → 抛(不是静默当"不可路由")。

    ★ 变异:把首行 `sticky_release_track` 换成 `sticky_release_track_or_none`
      → 本用例变红。
    """
    uc = make_uc(auth_repo=FakeAuthRepo())
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.assignment_routable(PLAYER, assignment(track="prod"))
    assert ei.value.code == errcode.ErrInvalidState


async def test_assignment_routable_legacy_reads_shard_mirror() -> None:
    """legacy 面从分片镜像判定;非 ready / 轨不符 / 镜像缺失都判不可路由。

    ★ 变异:把 `if shard is None or shard.state != STATE_READY` 里的 state 判据
      删掉 → warming 子用例变红。
    """
    a = assignment()
    uc = make_uc(repo=FakeRepo([shard(count=3, cap=9, addr="1.1.1.1:1")]))
    info, ok = await uc.assignment_routable(PLAYER, a)
    assert ok is True
    assert (info.hub_addr, info.player_count, info.capacity) == ("1.1.1.1:1", 3, 9)

    uc2 = make_uc(repo=FakeRepo([shard(state=STATE_WARMING)]))
    assert (await uc2.assignment_routable(PLAYER, a))[1] is False

    uc3 = make_uc(repo=FakeRepo([shard(track=releasetrack.CANARY)]))
    assert (await uc3.assignment_routable(PLAYER, a))[1] is False

    uc4 = make_uc(repo=FakeRepo([]))
    assert (await uc4.assignment_routable(PLAYER, a))[1] is False


# ═══════════════════════════════════════════════════════════════════════════
# 11. select_and_reserve_shard
# ═══════════════════════════════════════════════════════════════════════════


async def test_select_and_reserve_rejects_invalid_track() -> None:
    """非法轨在读分片**之前**拒。

    ★ 变异:删掉入口的 `releasetrack.valid` 判据 → 本用例变红。
    """
    repo = FakeRepo([shard()])
    uc = make_uc(repo=repo)
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.select_and_reserve_shard(PLAYER, AID, "global", 0, "", "prod")
    assert ei.value.code == errcode.ErrInvalidArg


async def test_select_and_reserve_returns_authoritative_view() -> None:
    """成功时返回的分片必须是被权威 seat 覆盖过的视图,不是 Redis 镜像原值。

    镜像里的 addr / 人数可能已过时;把过时 addr 发给客户端 = 连到一台已经换端口的 DS。

    ★ 变异:把 `return authoritative_shard(candidate, seat)` 改成
      `return candidate, seat` → 本用例变红。
    """
    repo = FakeRepo([shard(count=0, addr="stale:1")])
    auth = FakeAuthRepo(reserve=seat(addr="fresh:2", count=7, cap=10))
    uc = make_uc(repo=repo, auth_repo=auth)
    chosen, taken = await uc.select_and_reserve_shard(PLAYER, AID, "global", 0, "", "stable")
    assert chosen.hub_addr == "fresh:2"
    assert chosen.player_count == 7
    assert taken is auth.reserve
    assert repo.shards[0].hub_addr == "stale:1"  # 镜像未被就地改写


async def test_select_and_reserve_puts_team_shard_first() -> None:
    """队伍提示命中的候选被换到队首(负载序之上的偏好)。

    ★ 变异:把 `candidates[0], candidates[i] = candidates[i], candidates[0]` 删掉
      → 本用例变红(会选到人少的那台,队友被打散)。
    """
    repo = FakeRepo(
        [shard("a", shard_id=1, count=0), shard("b", shard_id=2, count=5)], team_shard="b"
    )
    auth = FakeAuthRepo()
    uc = make_uc(repo=repo, auth_repo=auth)
    await uc.select_and_reserve_shard(PLAYER, AID, "global", 42, "", "stable")
    assert auth.reserved[0][0] == "b"


async def test_select_and_reserve_tries_next_candidate_after_reserve_rejection() -> None:
    """某候选被原子门拒 → 计入 `reserve_rejected` 并试下一个,不整条失败。

    ★ 变异:把 `census.reserve_rejected += 1; continue` 改成 `raise` → 本用例变红。
    """
    repo = FakeRepo([shard("a", shard_id=1, count=0), shard("b", shard_id=2, count=1)])
    auth = FakeAuthRepo()
    auth.routable_by_pod = {"a": seat(ok=False, reason="heartbeat-stale"), "b": seat()}
    uc = make_uc(repo=repo, auth_repo=auth)
    chosen, _ = await uc.select_and_reserve_shard(PLAYER, AID, "global", 0, "", "stable")
    assert chosen.hub_pod_name == "b"


async def test_select_and_reserve_exhausted_reports_reserve_rejected_reason() -> None:
    """所有候选都被原子门拒 → `all_candidates_reserve_rejected`(不是 `all_shards_full`)。

    这两个 reason 的处置完全不同:前者查 DS 授权 / 心跳,后者才是扩容。

    ★ 变异:把 `no_routable_shard_reason` 的第 2 分支挪到第 4 分支之后 →
      本用例变红(会报成 no_shard_candidate)。
    """
    repo = FakeRepo([shard("a", shard_id=1, count=0)])
    auth = FakeAuthRepo(routable=seat(ok=False, reason="auth-missing"))
    uc = make_uc(repo=repo, auth_repo=auth)
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.select_and_reserve_shard(PLAYER, AID, "global", 0, "", "stable")
    assert ei.value.code == errcode.ErrHubNoAvailable


async def test_select_and_reserve_excludes_pod_and_wrong_track() -> None:
    """排除 pod / 轨不符 / region 不符的分片都进不了候选集。

    ★ 变异:把 `elif shard.hub_pod_name == exclude_pod:` 那支删掉 →
      第一个断言变红(切线会切回原地)。
    """
    repo = FakeRepo(
        [
            shard("a", shard_id=1, count=0),
            shard("b", shard_id=2, count=0, track=releasetrack.CANARY),
            shard("c", shard_id=3, count=0, region="cn"),
        ]
    )
    auth = FakeAuthRepo()
    uc = make_uc(repo=repo, auth_repo=auth)
    with pytest.raises(errcode.PandoraError):
        await uc.select_and_reserve_shard(PLAYER, AID, "global", 0, "a", "stable")
    assert auth.reserved == []


async def test_select_and_reserve_propagates_non_capacity_error() -> None:
    """非容量类错误(存储故障)必须上抛,不能被当成"这台不行,换下一台"。

    把存储故障吞成容量拒绝会让一次 Redis 抖动表现成"全服没有可用 hub"。

    ★ 变异:把 `if errcode.as_code(exc) == errcode.ErrHubNoAvailable` 改成
      无条件 `continue` → 本用例变红。
    """
    repo = FakeRepo([shard("a", shard_id=1, count=0)])
    auth = FakeAuthRepo()
    auth.routable_by_pod = {}

    class Boom(FakeAuthRepo):
        async def check_routable(self, pod, at_ms, max_age_ms):
            raise errcode.PandoraError(errcode.ErrInternal, "redis exploded")

    uc = make_uc(repo=repo, auth_repo=Boom())
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.select_and_reserve_shard(PLAYER, AID, "global", 0, "", "stable")
    assert ei.value.code == errcode.ErrInternal


# ═══════════════════════════════════════════════════════════════════════════
# 12. 补偿退座 / 视图
# ═══════════════════════════════════════════════════════════════════════════


async def test_compensate_reserved_seat_is_noop_without_seat() -> None:
    """Model B 下 `seat is None` = 根本没占到座,不许拿零值身份去 exact 退座。

    ★ 变异:把 `if seat is None: return` 删掉 → "未调用退座"断言变红。
    """
    auth = FakeAuthRepo()
    uc = make_uc(auth_repo=auth)
    await uc.compensate_reserved_seat(POD, PLAYER, AID, None)
    assert auth.released == []


async def test_compensate_reserved_seat_legacy_gives_back_integer_seat() -> None:
    """legacy 面走整数退座。

    ★ 变异:把 `if self.auth_repo is None:` 分支删掉 → 计数不回退,本用例变红。
    """
    s = shard(count=3)
    uc = make_uc(repo=FakeRepo([s]))
    await uc.compensate_reserved_seat(POD, PLAYER, AID, seat())
    assert s.player_count == 2


async def test_compensate_reserved_seat_survives_release_failure() -> None:
    """补偿失败只告警,**不抛** —— 抛出去会掩盖调用方原本要报的那个错。

    ★ 变异:把 `except BaseException as err: exc = err` 改成 `raise` → 本用例变红。
    """
    auth = FakeAuthRepo(release_error=RuntimeError("redis down"))
    uc = make_uc(auth_repo=auth)
    await uc.compensate_reserved_seat(POD, PLAYER, AID, seat())


async def test_release_assignment_seat_uses_exact_identity() -> None:
    """退座身份必须来自归属记录的 exact 四元组;`released=False` 不抛(同名 Pod 重建后正常)。

    ★ 变异:把 `assignment_instance_identity(assignment)` 换成
      `L.AssignmentInstanceIdentity()` → 身份断言变红。
    """
    auth = FakeAuthRepo(release_ok=False)
    uc = make_uc(auth_repo=auth)
    await uc.release_assignment_seat(assignment())
    pod, ident = auth.released[0]
    assert pod == POD
    assert (ident.assignment_id, ident.instance_uid, ident.protocol_epoch) == (AID, UID, 7)


async def test_routable_shard_views_filters_and_passes_through_legacy() -> None:
    """legacy 原样返回;Model B 下剔除非 ready 与不可路由的分片。

    对外展示一台"回调会被全拒"的 Hub = 玩家点进去必然失败。

    ★ 变异:把 `if info.ok:` 删掉 → 第二个断言变红。
    """
    shards = [shard("a", shard_id=1), shard("b", shard_id=2), shard("c", state=STATE_WARMING)]
    uc_legacy = make_uc()
    assert await uc_legacy.routable_shard_views(shards) is shards

    auth = FakeAuthRepo()
    auth.routable_by_pod = {"a": seat(addr="A:1"), "b": seat(ok=False)}
    uc = make_uc(auth_repo=auth)
    out = await uc.routable_shard_views(shards)
    assert [s.hub_pod_name for s in out] == ["a"]
    assert out[0].hub_addr == "A:1"


# ═══════════════════════════════════════════════════════════════════════════
# 13. replace_assignment_saga
# ═══════════════════════════════════════════════════════════════════════════


class FailingFence:
    """未持有写者租约 → `mint_source_revision` fail-closed。"""

    def current(self) -> tuple[bool, int]:
        return False, 0


class HeldFence:
    def current(self) -> tuple[bool, int]:
        return True, 5


async def test_replace_saga_compensates_when_source_revision_mint_fails() -> None:
    """铸号失败 = 整笔放弃 + 补偿座位,**绝不带着 0 号继续**。

    带 0 走下去会在已建立水位的玩家上被 owner 按 legacy 拒掉 —— 白白经历一次
    占座 + 补偿,而错误发生在更远的地方,排查会指向 owner。

    ★ 变异:把 `mint_source_revision` 失败分支里的 `raise` 改成
      `revision = 0` → 本用例变红(既不补偿也不抛)。
    """
    repo = FakeRepo()
    auth = FakeAuthRepo()
    uc = make_uc(repo=repo, auth_repo=auth)
    uc.writer_fence = FailingFence()
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.replace_assignment_saga(PLAYER, None, assignment(), seat(), None, "gone")
    assert ei.value.code == errcode.ErrUnavailable
    assert len(auth.released) == 1  # 座位已补偿
    assert repo.cas_calls == []  # 没有触碰归属


async def test_replace_saga_new_owner_happy_path() -> None:
    """新建(old=None):CAS → 加成员索引 → on_swapped → 写者复核。

    ★ 变异:把 `await self.add_shard_member(...)` 删掉 → 成员断言变红
      (强制整合从此枚举不到这个玩家)。
    """
    repo = FakeRepo()
    uc = make_uc(repo=repo, auth_repo=FakeAuthRepo())
    uc.writer_fence = HeldFence()
    calls: list[str] = []
    nxt = assignment(source_revision=0)
    retry = await uc.replace_assignment_saga(
        PLAYER, None, nxt, seat(), lambda: calls.append("swapped"), "gone"
    )
    assert retry is False
    assert nxt.source_revision != 0  # 领到号了
    assert uc.added == [(POD, PLAYER)]
    assert calls == ["swapped"]
    assert repo.registered == []  # 无旧 owner → 不登记 cleanup


async def test_replace_saga_cas_loser_compensates_and_asks_for_retry() -> None:
    """CAS 输给并发写者 → 摘 ref + 退座 + 返回 `True`(让调用方重试)。

    这里最容易写错的是"输了也不摘 ref":孤儿 ref 会让 reconciler 反复去清理一个
    从未生效的 target。

    ★ 变异:把 `if not swapped:` 分支里的 `remove_transfer_cleanup_ref` 删掉 →
      本用例变红。
    """
    old = assignment(aid="old-aid", pod="hub-old", uid="uid-old")
    repo = FakeRepo(assignment_rec=old)
    repo.cas_results = [False]
    auth = FakeAuthRepo()
    uc = make_uc(repo=repo, auth_repo=auth)
    uc.writer_fence = HeldFence()
    nxt = assignment(aid="new-aid", pod="hub-new", uid="uid-new")
    retry = await uc.replace_assignment_saga(PLAYER, old, nxt, seat(), None, "gone")
    assert retry is True
    assert repo.registered == [("hub-old", R.TransferCleanupRef(PLAYER, "new-aid"))]
    assert repo.removed_refs == [("hub-old", R.TransferCleanupRef(PLAYER, "new-aid"))]
    assert len(auth.released) == 1
    assert uc.added == []


async def test_replace_saga_registers_cleanup_before_cas() -> None:
    """index-first:清理 ref 必须在 assignment CAS **之前**登记。

    反过来做(先 CAS 再登记)会出现"新 owner 已生效、旧 owner 没人清"的永久残留 ——
    而崩溃 / CAS loser 至多留下一条可安全识别的孤儿 ref。

    ★ 变异:把 `register_transfer_cleanup` 调用挪到 CAS 之后 → 本用例的顺序断言变红。
    """
    order: list[str] = []
    old = assignment(aid="old-aid", pod="hub-old", uid="uid-old")

    class OrderRepo(FakeRepo):
        async def register_transfer_cleanup(self, source_pod, ref):
            order.append("register")
            await super().register_transfer_cleanup(source_pod, ref)

        async def compare_and_swap_assignment(self, player_id, expected, next_rec, ttl):
            order.append("cas")
            return await super().compare_and_swap_assignment(player_id, expected, next_rec, ttl)

    repo = OrderRepo(assignment_rec=old)
    uc = make_uc(repo=repo, auth_repo=FakeAuthRepo())
    uc.writer_fence = HeldFence()
    nxt = assignment(aid="new-aid", pod="hub-new", uid="uid-new")
    # CAS 成功后 resume 会读到 repo.assignment(= nxt,已带 cleanup 相位)。
    await uc.replace_assignment_saga(PLAYER, old, nxt, seat(), None, "gone")
    assert order[:2] == ["register", "cas"]


async def test_replace_saga_keeps_ref_when_cas_outcome_unknown() -> None:
    """CAS 抛异常(结果**未知**)时保留 ref 与 reservation,不盲目补偿。

    盲目补偿会退掉一个**可能已经生效**的新 owner 的座位。

    ★ 变异:把 `if not cleanup_registered:` 判据删掉(改成无条件补偿)→
      "未退座"断言变红。
    """
    old = assignment(aid="old-aid", pod="hub-old", uid="uid-old")
    repo = FakeRepo(assignment_rec=old)
    repo.cas_error = RuntimeError("redis timeout")
    auth = FakeAuthRepo()
    uc = make_uc(repo=repo, auth_repo=auth)
    uc.writer_fence = HeldFence()
    with pytest.raises(RuntimeError):
        await uc.replace_assignment_saga(
            PLAYER, old, assignment(aid="new-aid", pod="hub-new", uid="uid-new"), seat(), None, "gone"
        )
    assert auth.released == []
    assert repo.removed_refs == []


async def test_replace_saga_legacy_releases_old_seat_directly() -> None:
    """legacy(无 auth_repo)不走 cleanup saga,直接退旧座 + 摘成员索引。

    ★ 变异:把 `elif old is not None:` 分支删掉 → 成员移除断言变红(旧分片人数永不回落)。
    """
    old = assignment(aid="old-aid", pod="hub-old")
    s_old = shard("hub-old", count=3)
    repo = FakeRepo([s_old], assignment_rec=old)
    uc = make_uc(repo=repo)
    retry = await uc.replace_assignment_saga(
        PLAYER, old, assignment(aid="new-aid", pod="hub-new"), None, None, "gone"
    )
    assert retry is False
    assert s_old.player_count == 2
    assert uc.member_removed == [("hub-old", PLAYER)]


# ═══════════════════════════════════════════════════════════════════════════
# 14. resume_assignment_cleanup
# ═══════════════════════════════════════════════════════════════════════════


def transfer_pending(*, target_bound: bool = False):
    """造一份处于 transfer cleanup 相位的归属记录。"""
    target = assignment(aid="new-aid", pod="hub-new", uid="uid-new")
    bs.bind_transfer_cleanup_source(
        target, assignment(aid="old-aid", pod="hub-old", uid="uid-old", epoch=5)
    )
    target.transfer_target_bound = target_bound
    return target


async def test_resume_cleanup_requires_identity_and_authority() -> None:
    """身份缺失 → ErrInvalidArg;无权威 → ErrUnavailable(两者不能混)。

    ★ 变异:把 `if self.auth_repo is None` 那支删掉 → 第三个子用例变红
      (会拿 None 去调 inspect,抛 AttributeError 而不是可重试的 ErrUnavailable)。
    """
    uc = make_uc(auth_repo=FakeAuthRepo())
    for pid, aid in ((0, AID), (PLAYER, "")):
        with pytest.raises(errcode.PandoraError) as ei:
            await uc.resume_assignment_cleanup(pid, aid)
        assert ei.value.code == errcode.ErrInvalidArg
    with pytest.raises(errcode.PandoraError) as ei:
        await make_uc().resume_assignment_cleanup(PLAYER, AID)
    assert ei.value.code == errcode.ErrUnavailable


async def test_resume_cleanup_missing_assignment_is_terminal_not_error() -> None:
    """归属已不存在 → `(None, False)`,不抛。

    ★ 变异:把 `return None, False` 改成 `raise` → 本用例变红。
    """
    uc = make_uc(repo=FakeRepo(assignment_rec=None), auth_repo=FakeAuthRepo())
    assert await uc.resume_assignment_cleanup(PLAYER, AID) == (None, False)


async def test_resume_cleanup_refuses_superseded_assignment() -> None:
    """assignment_id 已变 → ErrLocatorConflict,**绝不触碰**。

    继续清理会删掉刚刚胜出的 winner —— 这是最凶的一种"清理把在场玩家踢掉"。

    ★ 变异:把 `if current.assignment_id != assignment_id:` 删掉 → 本用例变红。
    """
    uc = make_uc(repo=FakeRepo(assignment_rec=assignment(aid="newer")), auth_repo=FakeAuthRepo())
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.resume_assignment_cleanup(PLAYER, AID)
    assert ei.value.code == errcode.ErrLocatorConflict


async def test_resume_cleanup_rejects_conflicting_phases() -> None:
    """两个 cleanup 相位并存 = 记录被写坏,fail-closed。

    ★ 变异:删掉两相位并存的判据 → 本用例变红(会按 release 分支去删归属,
      而 transfer source 永远没人清)。
    """
    rec = transfer_pending()
    rec.release_cleanup_pending = True
    uc = make_uc(repo=FakeRepo(assignment_rec=rec), auth_repo=FakeAuthRepo())
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.resume_assignment_cleanup(PLAYER, "new-aid")
    assert ei.value.code == errcode.ErrInvalidState


@pytest.mark.parametrize(
    "field",
    [
        "transfer_target_bound",
        "transfer_source_hub_pod_name",
        "transfer_source_assignment_id",
        "transfer_source_instance_uid",
        "transfer_source_auth_epoch",
        "transfer_source_auth_writer_epoch",
        "release_cleanup_match_id",
        "release_cleanup_placement_version",
        "release_cleanup_operation_id",
    ],
)
async def test_resume_cleanup_rejects_orphan_phase_fields(field: str) -> None:
    """没有 pending 相位却残留任何一格 saga 字段 → fail-closed。

    这九格就是 `clear_transfer_cleanup` 必须全清的理由:漏清一格,该玩家从此
    每次进场都在这里被拒。

    ★ 变异:删掉 orphan 判据里对应的那一条 `or` → 对应用例变红。
    """
    rec = assignment()
    setattr(rec, field, 1 if isinstance(getattr(rec, field), (bool, int)) else "x")
    uc = make_uc(repo=FakeRepo(assignment_rec=rec), auth_repo=FakeAuthRepo())
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.resume_assignment_cleanup(PLAYER, AID)
    assert ei.value.code == errcode.ErrInvalidState


async def test_resume_cleanup_clean_record_passes_through() -> None:
    """无 cleanup 相位、无孤儿字段 → 原样返回 `(rec, True)`。

    ★ 变异:把 `return current, True` 改成 `return None, False` → 本用例变红
      (调用方会把它当成"归属消失了"而报 ErrInvalidState)。
    """
    rec = assignment()
    uc = make_uc(repo=FakeRepo(assignment_rec=rec), auth_repo=FakeAuthRepo())
    assert await uc.resume_assignment_cleanup(PLAYER, AID) == (rec, True)


async def test_resume_cleanup_transfer_marks_bind_then_releases_source() -> None:
    """transfer 相位完整走完:标记 target_bound → 精确退旧座 → 清相位 → 摘索引。

    ★ 变异:把 `clear_transfer_cleanup(next_rec)` 删掉 → "相位已清"断言变红
      (记录会永远停在 pending,每次进场都要重跑清理)。
    """
    rec = transfer_pending()
    repo = FakeRepo(assignment_rec=rec)
    auth = FakeAuthRepo()
    uc = make_uc(repo=repo, auth_repo=auth)
    out, found = await uc.resume_assignment_cleanup(PLAYER, "new-aid")
    assert found is True
    assert out.transfer_cleanup_pending is False
    assert out.transfer_source_hub_pod_name == ""
    # 退的是**源**分片的座位,身份取自 cleanup 字段而不是 target。
    pod, ident = auth.exact_calls[0]
    assert pod == "hub-old"
    assert (ident.assignment_id, ident.instance_uid, ident.protocol_epoch) == (
        "old-aid",
        "uid-old",
        5,
    )
    assert uc.member_removed == [("hub-old", PLAYER)]
    assert repo.removed_refs == [("hub-old", R.TransferCleanupRef(PLAYER, "new-aid"))]


async def test_resume_cleanup_stops_when_source_became_connected() -> None:
    """`departure_required` = 源 Hub 上真有一条活连接 → ErrUnavailable,**不删记录**。

    容量账本清理不是物理驱逐证明。删了就等于同一玩家两台可玩 DS(§9.22 脑裂)。

    ★ 变异:把 `if result.departure_required:` 那支删掉 → 本用例变红
      (会继续往下走并清掉相位,而旧 DS 上的连接还在)。
    """
    rec = transfer_pending(target_bound=True)
    auth = FakeAuthRepo(
        inspect=L.AssignmentSeatSnapshot(connected=True),
        exact=L.ReleaseAssignmentSeatResult(departure_required=True),
    )
    uc = make_uc(repo=FakeRepo(assignment_rec=rec), auth_repo=auth)
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.resume_assignment_cleanup(PLAYER, "new-aid")
    assert ei.value.code == errcode.ErrUnavailable
    assert rec.transfer_cleanup_pending is True


async def test_resume_cleanup_rejects_seat_owner_conflict() -> None:
    """seat 快照 conflict(座位属于**别的** owner)→ fail-closed,绝不硬退。

    ★ 变异:把 `if seat.conflict or (...)` 里的 `seat.conflict` 删掉 → 本用例变红。
    """
    rec = transfer_pending(target_bound=True)
    auth = FakeAuthRepo(inspect=L.AssignmentSeatSnapshot(conflict=True))
    uc = make_uc(repo=FakeRepo(assignment_rec=rec), auth_repo=auth)
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.resume_assignment_cleanup(PLAYER, "new-aid")
    assert ei.value.code == errcode.ErrInvalidState
    assert auth.exact_calls == []


async def test_resume_cleanup_release_phase_deletes_assignment() -> None:
    """release 相位:确认 exact owner 已退 → CAS DEL → 摘成员 + 摘索引。

    ★ 变异:把 `compare_and_swap_assignment(player_id, current, None, 0)` 的
      `None` 改成 `current` → "归属已删"断言变红(墓碑没落,玩家永远登不回来)。
    """
    rec = assignment()
    rec.release_cleanup_pending = True
    repo = FakeRepo(assignment_rec=rec)
    auth = FakeAuthRepo(exact=L.ReleaseAssignmentSeatResult(already_absent=True))
    uc = make_uc(repo=repo, auth_repo=auth)
    assert await uc.resume_assignment_cleanup(PLAYER, AID) == (None, False)
    assert repo.cas_calls[-1][2] is None
    assert uc.member_removed == [(POD, PLAYER)]
    assert repo.removed_refs == [(POD, R.TransferCleanupRef(PLAYER, AID))]


async def test_resume_cleanup_cas_retry_is_bounded() -> None:
    """CAS 恒失败时**有界**退出(ErrInternal),不无限自旋。

    §9.19/§9.23:每个等待都要有 deadline,否则一次持续竞争就把请求线程钉死。

    ★ 变异:把 `for _attempt in range(16)` 改成 `while True` → 本用例挂死(超时红)。
    """
    rec = transfer_pending()
    repo = FakeRepo(assignment_rec=rec)
    repo.cas_results = [False] * 100
    uc = make_uc(repo=repo, auth_repo=FakeAuthRepo())
    with pytest.raises(errcode.PandoraError) as ei:
        await asyncio.wait_for(uc.resume_assignment_cleanup(PLAYER, "new-aid"), timeout=5)
    assert ei.value.code == errcode.ErrInternal
    assert len(repo.cas_calls) == 16


async def test_register_transfer_cleanup_refuses_stacked_saga() -> None:
    """源记录自身还挂着未完成 cleanup → 拒绝叠加(否则两条 saga 互相覆盖)。

    ★ 变异:删掉 `if source.transfer_cleanup_pending or source.release_cleanup_pending`
      → 本用例变红。
    """
    uc = make_uc(repo=FakeRepo(), auth_repo=FakeAuthRepo())
    src = assignment(aid="old-aid", pod="hub-old")
    src.transfer_cleanup_pending = True
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.register_transfer_cleanup(assignment(aid="new-aid", pod="hub-new"), src)
    assert ei.value.code == errcode.ErrInvalidState


async def test_remove_transfer_cleanup_ref_is_best_effort() -> None:
    """摘索引失败只告警(残留交 reconciler 兜底),不打断调用链。

    ★ 变异:把 `except BaseException as exc:` 改成 `raise` → 本用例变红。
    """
    repo = FakeRepo()
    repo.remove_ref_error = RuntimeError("redis down")
    uc = make_uc(repo=repo)
    await uc.remove_transfer_cleanup_ref("hub-old", R.TransferCleanupRef(PLAYER, AID))


# ═══════════════════════════════════════════════════════════════════════════
# 15. 票据 / owner 绑定
# ═══════════════════════════════════════════════════════════════════════════


async def test_prepare_ticket_requires_signer() -> None:
    """没有签名器 → ErrUnavailable(可重试),不是 Internal。

    ★ 变异:把 `if assignment is None or self.signer is None` 删掉 →
      本用例抛 AttributeError 变红。
    """
    uc = make_uc()
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.prepare_hub_ticket(PLAYER, 1, assignment(), 0, "")
    assert ei.value.code == errcode.ErrUnavailable
    uc2 = make_uc(signer=FakeSigner())
    with pytest.raises(errcode.PandoraError):
        await uc2.prepare_hub_ticket(PLAYER, 1, None, 0, "")


async def test_prepare_ticket_refuses_incomplete_binding_under_model_b() -> None:
    """Model B 下不完整绑定不许签票(§9.22 fail-closed)。

    ★ 变异:删掉 `assignment_binding_v2_complete` 那道门 → 本用例变红
      (会签出一张 UE 侧必然拒绝的票,玩家陷入重连循环)。
    """
    signer = FakeSigner()
    uc = make_uc(signer=signer, auth_repo=FakeAuthRepo())
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.prepare_hub_ticket(PLAYER, 1, assignment(uid=""), 0, "")
    assert ei.value.code == errcode.ErrInvalidState
    assert signer.calls == []


async def test_prepare_ticket_wraps_signer_failure_as_internal() -> None:
    """签名失败 → ErrInternal,并保留 cause 便于排查。

    ★ 变异:把 `raise errcode.PandoraError(errcode.ErrInternal, ...)` 改成
      裸 `raise` → code 断言变红。
    """
    uc = make_uc(signer=FakeSigner(error=RuntimeError("no key")), auth_repo=FakeAuthRepo())
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.prepare_hub_ticket(PLAYER, 1, assignment(), 0, "")
    assert ei.value.code == errcode.ErrInternal
    assert isinstance(ei.value.cause, RuntimeError)


async def test_prepare_ticket_carries_session_and_match_binding() -> None:
    """`source_match_id` / `session_jti` 必须盖进 binding(§9.23 会话 fencing)。

    漏盖 sjti = 旧会话签出的票在顶号后仍然可用。

    ★ 变异:把 `binding.session_jti = session_jti` 删掉 → 本用例变红。
    """
    signer = FakeSigner(token="T", exp_ms=123)
    uc = make_uc(signer=signer, auth_repo=FakeAuthRepo())
    prepared = await uc.prepare_hub_ticket(PLAYER, 9, assignment(), 777, "sjti-x")
    _, role, binding = signer.calls[0]
    assert role == 9
    assert binding.source_match_id == 777
    assert binding.session_jti == "sjti-x"
    assert (prepared.token, prepared.expires_at_ms) == ("T", 123)
    assert prepared.target_ok is True
    assert prepared.owner_target.source_revision == 4242


async def test_prepare_ticket_refuses_without_exact_owner_identity() -> None:
    """Model B 下拼不出 exact owner 身份 → 拒签(不许拿不完整身份写权威)。

    构造:绑定完整(过了第一道门),但持久化轨在 owner 目标阶段被判非法。

    ★ 变异:删掉 `if not target_ok and self.auth_repo is not None:` → 本用例变红。
    """

    class NoTargetHarness(Harness):
        async def owner_target_for_hub_ticket(self, a):
            return OwnerTargetView(), False

    uc = NoTargetHarness(FakeRepo(), FakeFleet(), FakeSigner(), make_cfg())
    uc.auth_repo = FakeAuthRepo()
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.prepare_hub_ticket(PLAYER, 1, assignment(), 0, "")
    assert ei.value.code == errcode.ErrInvalidState


async def test_owner_target_model_b_does_not_fall_back_to_shard_mirror() -> None:
    """Model B 下缺 uid/epoch **不许**回源分片镜像:授权权威是授权记录,镜像只是投影。

    ★ 变异:把 `if self.auth_repo is not None: return OwnerTargetView(), False` 删掉
      → 本用例变红(会把投影当权威,两者分叉时静默写错 owner)。
    """
    repo = FakeRepo([shard(uid="uid-mirror", epoch=3)])
    uc = make_uc(repo=repo, auth_repo=FakeAuthRepo())
    assert await uc.owner_target_for_hub_ticket(assignment(uid="", epoch=0)) == (
        OwnerTargetView(),
        False,
    )


async def test_owner_target_legacy_falls_back_to_shard_mirror() -> None:
    """legacy(mode=local)回源镜像补 uid/epoch;镜像也没有身份(mock)→ False。

    不补的话 owner 权威里永远没有记录,而 login 的 §9.23 query-first 回查以 owner
    为第一权威:玩家选完角、Hub 也分配成功,`GetResumeContext` 仍恒落 WAIT,
    永远进不去大厅。

    ★ 变异:把回源那段删掉 → 第一个断言变红。
    """
    a = assignment(uid="", epoch=0)
    uc = make_uc(repo=FakeRepo([shard(uid="uid-mirror", epoch=3)]))
    target, ok = await uc.owner_target_for_hub_ticket(a)
    assert ok is True
    assert (target.instance_uid, target.instance_epoch) == ("uid-mirror", 3)
    assert target.source_revision == 4242  # 取自 assignment,不是现铸

    uc_mock = make_uc(repo=FakeRepo([shard(uid="", epoch=0)]))
    assert (await uc_mock.owner_target_for_hub_ticket(a))[1] is False

    uc_none = make_uc(repo=FakeRepo([]))
    assert (await uc_none.owner_target_for_hub_ticket(a))[1] is False


async def test_owner_target_rejects_invalid_track_and_empty_identity() -> None:
    """轨非法 / 缺 pod / 缺 assignment_id → False(不签、不写权威)。

    ★ 变异:删掉入口的三条判据之一 → 对应断言变红。
    """
    uc = make_uc(auth_repo=FakeAuthRepo())
    for a in (assignment(track="prod"), assignment(pod=""), assignment(aid="")):
        assert (await uc.owner_target_for_hub_ticket(a))[1] is False


async def test_bind_owner_rejects_when_assignment_changed_before_begin() -> None:
    """guard 发现 Redis 里的归属已经不是本次那份 → ErrUnavailable,票扣住不发。

    这是"CAS loser 永远碰不到 owner"的机械保证。

    ★ 变异:把 guard 里的 `hub_assignment_delivery_equal` 判据删掉 → 本用例变红。
    """
    repo = FakeRepo(assignment_rec=assignment(aid="someone-else"))
    uc = make_uc(repo=repo, signer=FakeSigner(), auth_repo=FakeAuthRepo(), owner_auth=FakeOwnerAuth())
    prepared = bs.PreparedHubTicket(
        token="T", expires_at_ms=1, owner_target=target_of(assignment()), target_ok=True
    )
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.bind_owner_for_published_hub_assignment(PLAYER, assignment(), prepared)
    assert ei.value.code == errcode.ErrUnavailable


async def test_bind_owner_rejects_when_owner_target_drifted() -> None:
    """归属交付等价、但 owner 目标已漂移(source_revision 变了)→ 同样扣票。

    ★ 变异:把 guard 里的 `owner_target_view_equal` 判据删掉 → 本用例变红。
    """
    published = assignment()
    repo = FakeRepo(assignment_rec=published)
    uc = make_uc(repo=repo, auth_repo=FakeAuthRepo(), owner_auth=FakeOwnerAuth())
    stale_target = dataclasses.replace(target_of(published), source_revision=1)
    prepared = bs.PreparedHubTicket(
        token="T", expires_at_ms=1, owner_target=stale_target, target_ok=True
    )
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.bind_owner_for_published_hub_assignment(PLAYER, published, prepared)
    assert ei.value.code == errcode.ErrUnavailable


async def test_bind_owner_happy_path_begins_exact_target() -> None:
    """happy:Begin 写入的目标就是 prepared 里那一份(逐格相同)。

    ★ 变异:把 `prepared.owner_target` 换成 `OwnerTargetView()` → 本用例变红。
    """
    published = assignment()
    owner = FakeOwnerAuth()
    uc = make_uc(repo=FakeRepo(assignment_rec=published), auth_repo=FakeAuthRepo(), owner_auth=owner)
    prepared = bs.PreparedHubTicket(
        token="T", expires_at_ms=1, owner_target=target_of(published), target_ok=True
    )
    await uc.bind_owner_for_published_hub_assignment(PLAYER, published, prepared)
    assert owner.begins == [target_of(published)]


async def test_bind_owner_propagates_begin_failure() -> None:
    """Begin 失败 → 上抛(扣票),绝不"先把票发了再说"。

    ★ 变异:把 owner Begin 的 `except ... raise` 改成 `pass` → 本用例变红。
    """
    published = assignment()
    owner = FakeOwnerAuth(non_exact=True)  # 权威回了一份非 exact 记录
    uc = make_uc(repo=FakeRepo(assignment_rec=published), auth_repo=FakeAuthRepo(), owner_auth=owner)
    prepared = bs.PreparedHubTicket(
        token="T", expires_at_ms=1, owner_target=target_of(published), target_ok=True
    )
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.bind_owner_for_published_hub_assignment(PLAYER, published, prepared)
    assert ei.value.code == errcode.ErrInvalidState


async def test_bind_owner_still_guards_when_target_not_ok() -> None:
    """`target_ok=False`(legacy)跳过 Begin,但**仍要**复核归属。

    跳过复核会让一次 CAS 落败后的旧票照样发出去。

    ★ 变异:把结尾的 `await guard()` 删掉 → 本用例变红。
    """
    repo = FakeRepo(assignment_rec=assignment(aid="someone-else"))
    uc = make_uc(repo=repo)
    prepared = bs.PreparedHubTicket(token="T", expires_at_ms=1, target_ok=False)
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.bind_owner_for_published_hub_assignment(PLAYER, assignment(), prepared)
    assert ei.value.code == errcode.ErrUnavailable


async def test_sign_result_assembles_from_assignment_and_ticket() -> None:
    """出参逐格取自归属记录 + 票据(地址不能取分片镜像,那份可能已过时)。

    ★ 变异:把 `hub_ds_addr=assignment.hub_addr` 改成 `""` → 本用例变红。
    """
    published = assignment(addr="7.7.7.7:70", shard_id=5)
    uc = make_uc(
        repo=FakeRepo(assignment_rec=published),
        signer=FakeSigner(token="TK", exp_ms=4242),
        auth_repo=FakeAuthRepo(),
        owner_auth=FakeOwnerAuth(),
    )
    out = await uc.sign_result(PLAYER, 9, published, 0, "")
    assert out.hub_ds_addr == "7.7.7.7:70"
    assert out.hub_ticket == "TK"
    assert out.hub_pod_name == POD
    assert out.shard_id == 5
    assert out.ticket_exp_ms == 4242


# ═══════════════════════════════════════════════════════════════════════════
# 16. 本地凭据 / owner 目标自愈
# ═══════════════════════════════════════════════════════════════════════════


async def test_local_ticket_binding_is_isolated_from_model_b() -> None:
    """Model B 面 / 非 local fleet 一律返回零值绑定(双重机械门)。

    ★ 变异:删掉 `if self.auth_repo is not None: return HubTicketBinding()` →
      第一个断言变红(线上会去读一个不该存在的本地凭据源)。
    """
    fleet = LocalCredFleet(cred=LocalCred())
    uc_modelb = make_uc(fleet=fleet, auth_repo=FakeAuthRepo())
    assert uc_modelb.local_ticket_binding(assignment()).pod_name == ""

    uc_plain = make_uc(fleet=FakeFleet())
    assert uc_plain.local_ticket_binding(assignment()).pod_name == ""


async def test_local_ticket_binding_fills_seven_tuple() -> None:
    """mode=local:从下发给本机 Hub DS 的**同一份**凭据拼出完整绑定。

    这不是伪造 —— 心跳应答 ACK 回显的就是它,与 DS 自持身份逐字段相等。

    ★ 变异:把 `credential_jti=cred.jti` 改成 `""` → 本用例变红
      (UE 的 IsBoundToRequest 会判 ACK 不完整,准入租约永不打开)。
    """
    fleet = LocalCredFleet(cred=LocalCred())
    uc = make_uc(fleet=fleet)
    b = uc.local_ticket_binding(assignment(uid="", epoch=0, gen=0, jti="", writer_epoch=0))
    assert b.pod_name == POD
    assert b.instance_uid == "local-uid"
    assert b.protocol_epoch == 11
    assert b.credential_gen == 12
    assert b.credential_jti == "local-jti"
    assert b.hub_assignment_id == AID
    assert b.writer_epoch == DS_AUTH_WRITER_EPOCH_V2
    assert b.release_track == releasetrack.STABLE


async def test_local_ticket_binding_refuses_half_credential() -> None:
    """凭据源返回 None(pod 不符 / 凭据不全)→ 零值绑定,不拼半截。

    ★ 变异:把 `if cred is None: return HubTicketBinding()` 删掉 →
      本用例抛 AttributeError 变红。
    """
    uc = make_uc(fleet=LocalCredFleet(cred=None))
    assert uc.local_ticket_binding(assignment()).pod_name == ""
    uc2 = make_uc(fleet=LocalCredFleet(cred=LocalCred()))
    assert uc2.local_ticket_binding(assignment(pod="other")).pod_name == ""


async def test_resolve_owner_target_carries_source_revision() -> None:
    """census 自愈目标与签票目标**同源**,且必须带上 assignment 的来源版本。

    漏带 = 自愈 Begin 恒发 legacy(0),而该玩家水位已非零,owner 按
    `legacy_after_versioned` 拒掉 —— 专为修漂移而建的通道 100% 失效。

    ★ 变异:把 `source_revision=assignment.source_revision` 删掉 → 本用例变红。
    """
    uc = make_uc(repo=FakeRepo(assignment_rec=assignment(source_revision=999)))
    target, ok = await uc.resolve_owner_target_from_assignment(PLAYER)
    assert ok is True
    assert target.source_revision == 999
    assert target == dataclasses.replace(
        target_of(assignment()), source_revision=999
    )


async def test_resolve_owner_target_refuses_incomplete_binding() -> None:
    """绑定不完整 / 无归属 / 读取失败 → 不自愈(返回 False)。

    ★ 变异:把 `if binding.pod_name == "" or binding.instance_uid == "":` 删掉 →
      第一个断言变红(会用零值身份去写 owner 权威)。
    """
    uc = make_uc(repo=FakeRepo(assignment_rec=assignment(uid="")))
    assert (await uc.resolve_owner_target_from_assignment(PLAYER))[1] is False

    uc_empty = make_uc(repo=FakeRepo(assignment_rec=None))
    assert (await uc_empty.resolve_owner_target_from_assignment(PLAYER))[1] is False


async def test_effective_role_id_prefers_explicit_request() -> None:
    """显式传入的 role_id 优先(login 是角色权威);否则回退归属镜像已存值。

    回退分支是 Transfer / 重签路径的唯一角色来源 —— login 不在那条环上。

    ★ 变异:把 `if requested > 0` 改成 `if requested >= 0` → 第二个断言变红。
    """
    assert bs.effective_role_id(7, 9) == 7
    assert bs.effective_role_id(0, 9) == 9
    assert bs.effective_role_id(0, 0) == 0
