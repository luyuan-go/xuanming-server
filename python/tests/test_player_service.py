"""player service 层 —— 鉴权矩阵与 in-band 错误码。

这一层没有业务逻辑,几乎全部风险集中在**三类鉴权边界互相串味**上:

  自助写 RPC  未鉴权直连 → UNAUTHORIZED;请求体 player_id 与调用者不一致 → PERMISSION_DENY
  读 RPC      内部直连信任请求体;客户端强制只能查自己
  系统 RPC    带玩家 JWT 一律拒

串味的后果都是 IDOR:把系统 RPC 写成自助写 → 玩家能给自己发卡 / 加点;把自助写写成读双模
→ 任何能连内网的东西都能改他人存档。这些分支返回的是 **in-band code + 正常响应**,
access log 只记 DEBUG —— 线上零可见性,只能靠测试钉。
"""

from __future__ import annotations

import pytest

from pandora.common.v1 import errcode_pb2 as commonpb
from pandora.player.v1 import player_pb2 as ppb

from pandorapy import dsauth, errcode, interceptors
from pandorapy.services.player import models as m
from pandorapy.services.player import service as psvc


class FakeContext:
    """最小 ServicerContext:只需要 invocation_metadata()。"""

    def __init__(self, player_id: int = 0, **headers: str) -> None:
        md: list[tuple[str, str]] = []
        if player_id:
            md.append((interceptors.METADATA_KEY_PLAYER_ID, str(player_id)))
        md.extend(headers.items())
        self._md = tuple(md)

    def invocation_metadata(self):  # noqa: ANN201
        return self._md


class FakeUC:
    """按脚本回放的 usecase。业务正确性在 test_player_biz.py 验,这里只验边界。"""

    def __init__(self, **results) -> None:
        self.results = results
        self.calls: list[tuple] = []

    def _ret(self, name: str, *args):  # noqa: ANN202
        self.calls.append((name, *args))
        value = self.results.get(name)
        if isinstance(value, BaseException):
            raise value
        return value

    async def get_profile(self, player_id):  # noqa: ANN001
        return self._ret("get_profile", player_id) or ppb.PlayerProfile(player_id=player_id)

    async def update_nickname(self, player_id, nickname):  # noqa: ANN001
        return self._ret("update_nickname", player_id, nickname)

    async def ensure_profile_named(self, player_id, nickname):  # noqa: ANN001
        self._ret("ensure_profile_named", player_id, nickname)
        return m.EnsureProfileResult(created=True, nickname=nickname or "Player_1", level=1)

    async def get_player_names(self, ids):  # noqa: ANN001
        self._ret("get_player_names", tuple(ids))
        return [m.PlayerName(player_id=i, nickname=f"n{i}") for i in ids]

    async def unlock_hero(self, player_id, hero_id, source):  # noqa: ANN001
        return self._ret("unlock_hero", player_id, hero_id, source)

    async def get_mmr(self, player_id, pool):  # noqa: ANN001
        self._ret("get_mmr", player_id, pool)
        return 1500, False

    async def update_mmr(self, player_id, delta, reason, key, pool):  # noqa: ANN001
        self._ret("update_mmr", player_id, delta, reason, key, pool)
        return 1515, False

    async def grant_skill_cards(self, player_id, grants, key):  # noqa: ANN001
        self._ret("grant_skill_cards", player_id, tuple(grants), key)
        return [], False

    async def add_experience(self, player_id, delta, reason, key):  # noqa: ANN001
        self._ret("add_experience", player_id, delta, reason, key)
        return m.ExpState(level=2, exp_in_level=1), False

    async def get_loadout(self, player_id):  # noqa: ANN001
        self._ret("get_loadout", player_id)
        return ppb.PlayerLoadout(player_id=player_id)

    async def set_equipment(self, player_id, slots):  # noqa: ANN001
        return self._ret("set_equipment", player_id, tuple(slots))

    async def get_attributes(self, player_id):  # noqa: ANN001
        self._ret("get_attributes", player_id)
        return [], 0


def _svc(uc=None, guard=None) -> psvc.PlayerService:  # noqa: ANN001
    return psvc.PlayerService(uc or FakeUC(), guard)


# ── 自助写 ───────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_self_write_rejects_unauthenticated_direct_call() -> None:
    """写接口不该被后端内部直连(那类操作走系统 RPC),故未鉴权一律拒。"""
    resp = await _svc().UpdateNickname(
        ppb.UpdateNicknameRequest(player_id=1, nickname="x"), FakeContext()
    )
    assert resp.code == commonpb.ERR_UNAUTHORIZED


@pytest.mark.asyncio
async def test_self_write_rejects_body_mismatch() -> None:
    resp = await _svc().UpdateNickname(
        ppb.UpdateNicknameRequest(player_id=999, nickname="x"), FakeContext(player_id=1)
    )
    assert resp.code == commonpb.ERR_PERMISSION_DENY


@pytest.mark.asyncio
async def test_self_write_uses_caller_identity_not_body() -> None:
    """★ 权威 player_id = 调用者身份;请求体只用来做一致性校验,不参与业务。"""
    uc = FakeUC()
    resp = await _svc(uc).UpdateNickname(
        ppb.UpdateNicknameRequest(player_id=0, nickname="新名"), FakeContext(player_id=42)
    )
    assert resp.code == commonpb.OK
    assert ("update_nickname", 42, "新名") in uc.calls


@pytest.mark.asyncio
async def test_set_equipment_ignores_client_supplied_attributes() -> None:
    """★ identified / attributes 是 inventory 域权威快照,**不从请求信任**。

    信了它,客户端就能给自己配一件任意词条的装备并直接进战斗。
    """
    from pandora.inventory.v1 import inventory_pb2 as inv_pb

    uc = FakeUC()
    req = ppb.SetEquipmentRequest(
        player_id=0,
        equipment=[
            ppb.LoadoutEquipment(
                slot=1,
                item_config_id=10,
                instance_id=99,
                identified=True,
                attributes=[inv_pb.ItemAttribute(attr_id=3, value=999_999)],
            )
        ],
    )
    await _svc(uc).SetEquipment(req, FakeContext(player_id=7))
    slots = [c for c in uc.calls if c[0] == "set_equipment"][-1][2]
    assert slots == (m.EquipmentSlot(slot=1, item_config_id=10, instance_id=99),)


# ── 读双模 ───────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_read_internal_call_trusts_body() -> None:
    uc = FakeUC()
    resp = await _svc(uc).GetAttributes(
        ppb.GetAttributesRequest(player_id=123), FakeContext()
    )
    assert resp.code == commonpb.OK
    assert ("get_attributes", 123) in uc.calls


@pytest.mark.asyncio
async def test_read_internal_call_requires_body_player_id() -> None:
    resp = await _svc().GetAttributes(ppb.GetAttributesRequest(), FakeContext())
    assert resp.code == commonpb.ERR_INVALID_ARG


@pytest.mark.asyncio
async def test_read_client_cannot_read_other_player() -> None:
    resp = await _svc().GetAttributes(
        ppb.GetAttributesRequest(player_id=999), FakeContext(player_id=1)
    )
    assert resp.code == commonpb.ERR_PERMISSION_DENY


# ── 系统 RPC ─────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("method", "request_factory"),
    [
        ("UnlockHero", lambda: ppb.UnlockHeroRequest(player_id=1, hero_id=2)),
        ("UpdateMMR", lambda: ppb.UpdateMMRRequest(player_id=1, idempotency_key="k")),
        ("EnsureProfile", lambda: ppb.EnsureProfileRequest(player_id=1)),
        ("GetPlayerNames", lambda: ppb.GetPlayerNamesRequest(player_ids=[1])),
        (
            "AddExperience",
            lambda: ppb.AddExperienceRequest(player_id=1, exp_delta=5, idempotency_key="k"),
        ),
        (
            "GrantSkillCards",
            lambda: ppb.GrantSkillCardsRequest(player_id=1, idempotency_key="k"),
        ),
        (
            "GrantAttributePoints",
            lambda: ppb.GrantAttributePointsRequest(player_id=1, points=1),
        ),
        ("GrantTalentPoints", lambda: ppb.GrantTalentPointsRequest(player_id=1, points=1)),
    ],
)
@pytest.mark.asyncio
async def test_system_rpcs_reject_client_jwt(method: str, request_factory) -> None:  # noqa: ANN001
    """★ 带玩家 JWT 调系统 RPC = 越权尝试。漏一条就是"玩家能给自己发卡/加点/改段位"。"""
    svc = _svc()
    resp = await getattr(svc, method)(request_factory(), FakeContext(player_id=5))
    assert resp.code == commonpb.ERR_PERMISSION_DENY


@pytest.mark.asyncio
async def test_ensure_profile_requires_player_id() -> None:
    resp = await _svc().EnsureProfile(ppb.EnsureProfileRequest(), FakeContext())
    assert resp.code == commonpb.ERR_INVALID_ARG


@pytest.mark.asyncio
async def test_update_mmr_requires_idempotency_key() -> None:
    """幂等键缺失 = 不变量 §2 无从保证,必须在触碰仓储前拒。"""
    resp = await _svc().UpdateMMR(ppb.UpdateMMRRequest(player_id=1), FakeContext())
    assert resp.code == commonpb.ERR_INVALID_ARG


@pytest.mark.asyncio
async def test_get_mmr_echoes_normalized_pool() -> None:
    resp = await _svc().GetMMR(
        ppb.GetMMRRequest(player_id=1, rating_pool="  "), FakeContext()
    )
    assert resp.code == commonpb.OK
    assert resp.rating_pool == "default"
    assert resp.found is False


# ── DS 面令牌门 ──────────────────────────────────────────────────────────────


def _enforce_guard() -> dsauth.DSCallbackGuard:
    verifier = dsauth.DSCallbackVerifier(
        issuer="pandora-ds-control",
        audience="pandora-ds",
        secret="pandora-dev-jwt-secret-change-me-32!",
        additional_secrets=[],
    )
    return dsauth.DSCallbackGuard(verifier, dsauth.Mode.ENFORCE)


@pytest.mark.asyncio
async def test_get_loadout_ds_gate_rejects_tokenless_internal_call() -> None:
    """require_token:全仓无内部无令牌调用方,故直连无令牌也拒。

    堵住「绕过 Envoy 直连业务端口、无标记无令牌却被当内部东西向信任」的旁路。
    """
    resp = await _svc(guard=_enforce_guard()).GetLoadout(
        ppb.GetLoadoutRequest(player_id=1), FakeContext()
    )
    assert resp.code == commonpb.ERR_UNAUTHORIZED


@pytest.mark.asyncio
async def test_get_loadout_ds_gate_is_skipped_for_authenticated_client() -> None:
    """★ 只在 caller==0 那一支过门。

    带玩家 JWT 的客户端(走 :8443)authorization 里是**玩家** JWT,拿去 DS 验签必然失败
    —— 无条件过门会把正常客户端一起拒掉。
    """
    uc = FakeUC()
    resp = await psvc.PlayerService(uc, _enforce_guard()).GetLoadout(
        ppb.GetLoadoutRequest(player_id=0), FakeContext(player_id=7)
    )
    assert resp.code == commonpb.OK
    assert ("get_loadout", 7) in uc.calls


@pytest.mark.asyncio
async def test_get_player_names_ds_gate_applies_too() -> None:
    """GetPlayerNames 是本服务第二条挂 DS 面的 RPC,门必须一样。"""
    resp = await _svc(guard=_enforce_guard()).GetPlayerNames(
        ppb.GetPlayerNamesRequest(player_ids=[1]), FakeContext()
    )
    assert resp.code == commonpb.ERR_UNAUTHORIZED


@pytest.mark.asyncio
async def test_permissive_guard_lets_call_through() -> None:
    """permissive 跑完整验签路径但失败只 warn 放行 —— dev 行为不变。"""
    verifier = dsauth.DSCallbackVerifier(
        issuer="pandora-ds-control",
        audience="pandora-ds",
        secret="pandora-dev-jwt-secret-change-me-32!",
        additional_secrets=[],
    )
    guard = dsauth.DSCallbackGuard(verifier, dsauth.Mode.PERMISSIVE)
    resp = await _svc(guard=guard).GetLoadout(
        ppb.GetLoadoutRequest(player_id=1), FakeContext()
    )
    assert resp.code == commonpb.OK


@pytest.mark.asyncio
async def test_no_guard_is_equivalent_to_mode_off() -> None:
    resp = await _svc().GetLoadout(ppb.GetLoadoutRequest(player_id=1), FakeContext())
    assert resp.code == commonpb.OK


# ── 错误码映射 ───────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_business_failure_returns_in_band_code_not_grpc_error() -> None:
    """★ 业务失败**不抛 gRPC 错误** —— 调用方按 code 分支,不靠 gRPC status 猜。"""
    uc = FakeUC(get_loadout=errcode.PandoraError(errcode.ErrPlayerNotFound, "nope"))
    resp = await _svc(uc).GetLoadout(ppb.GetLoadoutRequest(player_id=1), FakeContext())
    assert resp.code == commonpb.ERR_PLAYER_NOT_FOUND


@pytest.mark.asyncio
async def test_unexpected_exception_maps_to_internal() -> None:
    uc = FakeUC(get_loadout=RuntimeError("boom"))
    resp = await _svc(uc).GetLoadout(ppb.GetLoadoutRequest(player_id=1), FakeContext())
    assert resp.code == commonpb.ERR_INTERNAL


def test_all_proto_rpcs_are_implemented() -> None:
    """★ 方法名拼错的 RPC 实际返回 UNIMPLEMENTED,且启动期零信号。

    按生成的 servicer 基类枚举,漏一个当场变红。
    """
    from pandora.player.v1 import player_pb2_grpc as pgrpc

    base = pgrpc.PlayerServiceServicer
    expected = {
        name
        for name in dir(base)
        if not name.startswith("_") and callable(getattr(base, name))
    }
    missing = [
        name for name in expected if getattr(psvc.PlayerService, name) is getattr(base, name)
    ]
    assert not missing, f"未实现的 RPC: {missing}"
    assert len(expected) == 28, f"proto 的 RPC 数变了({len(expected)}),请同步本用例"
