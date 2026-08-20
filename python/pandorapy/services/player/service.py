"""player gRPC service 层 —— 对应 Go 侧 internal/service/{player,skill_card}.go。

职责:proto Request/Response ↔ biz 入参/出参互转,errcode → commonv1.ErrCode 1:1 映射。
业务失败一律走 **in-band code**(返回 response.code,gRPC status 保持 OK),与 Go 逐字一致。

鉴权边界(2026-07-08 安全审查:开放客户端入口前修 IDOR),三类各不相同:

  - **客户端自助写**(改昵称 / 选英雄 / 加点 / 洗点 / 出装 / 天赋 / 技能卡 / 领奖):
    以 Envoy jwt_authn 注入的调用者身份为准(`_self_player_id`),**不信任请求体
    player_id**;未鉴权直连或请求体与调用者不一致直接拒。
  - **客户端读**(档案 / 属性 / 出装 / 天赋 / 出战快照 / 领奖记录):双模
    (`_resolve_player_id`)—— 内部直连(caller==0)信任请求体;经 Envoy 的客户端
    (caller>0)强制只能查自己。
  - **系统 RPC**(UpdateMMR / UnlockHero / Grant* / EnsureProfile / GetPlayerNames /
    AddExperience):只允许后端内部直连(caller==0);带玩家 JWT 一律拒。

★ 挂在 Envoy **DS 面(:8444)** 的两条 RPC(GetLoadout / GetPlayerNames)另加一道 DS
  令牌门:该监听器没有 jwt_authn,caller 恒为 0,于是「caller==0 即后端内部可信」在这
  两条上不成立 —— 任何能连 :8444 或绕过 Envoy 直连 20002 的进程都能拿任意玩家的出战
  快照 / 批量捞名字。门只在 caller==0 那一支过:带玩家 JWT 的客户端(走 :8443)
  authorization 头里是**玩家** JWT,拿去 DS 验签必然失败,无条件过门会把正常客户端一起拒掉。
"""

from __future__ import annotations

import asyncio

from pandora.common.v1 import errcode_pb2 as commonpb
from pandora.player.v1 import player_pb2 as ppb
from pandora.player.v1 import player_pb2_grpc as pgrpc

from pandorapy import dsauth
from pandorapy import errcode
from pandorapy import internalrpcauth
from pandorapy import interceptors as pintercept
from pandorapy import log as plog
from pandorapy import rating as prating
from pandorapy.services.player import models as m

GRPC_SERVICE_FULL_NAME = "pandora.player.v1.PlayerService"
INTERNAL_GRPC_SERVICE_FULL_NAME = "pandora.player.v1.PlayerInternalService"
RESOLVE_PLAYER_NAMES_METHOD = (
    "/pandora.player.v1.PlayerInternalService/ResolvePlayerNames"
)
MAX_RESOLVE_PLAYER_NAMES = 32


def _internal_metadata_dict(context) -> dict[str, str]:  # noqa: ANN001
    """internalrpcauth 的单值 metadata 视图；重复键强制验签失败。"""
    out: dict[str, str] = {}
    duplicates: set[str] = set()
    for key, value in context.invocation_metadata() or ():
        if isinstance(value, bytes):
            continue
        normalized = key.lower()
        if normalized in out:
            duplicates.add(normalized)
        else:
            out[normalized] = str(value)
    for key in duplicates:
        out[key] = ""
    return out


def _code_of(exc: BaseException) -> int:
    """把内部异常映射成 in-band ErrCode(与 Go 的 errcode.As 同口径)。"""
    if isinstance(exc, errcode.PandoraError):
        return exc.code
    return errcode.ErrInternal


def _log_authz_deny(context, reason: str, caller_id: int, req_player_id: int) -> None:  # noqa: ANN001
    """记录鉴权拒绝(IDOR 尝试 / 越权调系统 RPC / 未鉴权直连写口)。

    这些是安全 fail-closed 分支,却以 response Code + 无 transport error 返回 → 统一
    access log 记 rpc_ok(DEBUG),线上对「伪造 player_id 改他人存档」「带玩家 JWT 调系统
    RPC」的尝试**零可见性**。本文件大段注释都在防 IDOR,拒绝事件必须可审计。
    """
    plog.get().warning(
        "player_authz_denied",
        reason=reason,
        caller_id=caller_id,
        req_player_id=req_player_id,
    )


def _self_player_id(context, req_player_id: int) -> tuple[int, int]:  # noqa: ANN001
    """客户端自助写 RPC 的权威 player_id。

        未鉴权(caller==0,直连内网无网关注入) → ERR_UNAUTHORIZED
        请求体 player_id 与调用者不一致        → ERR_PERMISSION_DENY

    返回 (权威 player_id, code)。写接口不该被后端内部直连(那类操作走系统 RPC),
    故未鉴权一律拒。
    """
    caller_id = pintercept.extract_player_id(context)
    if caller_id == 0:
        _log_authz_deny(context, "self_write_unauthenticated", caller_id, req_player_id)
        return 0, commonpb.ERR_UNAUTHORIZED
    if req_player_id != 0 and req_player_id != caller_id:
        _log_authz_deny(context, "self_write_player_id_mismatch", caller_id, req_player_id)
        return 0, commonpb.ERR_PERMISSION_DENY
    return caller_id, commonpb.OK


def _resolve_player_id(context, req_player_id: int) -> tuple[int, int]:  # noqa: ANN001
    """读接口双模取权威 player_id。

        内部直连(caller==0):信任请求体;body==0 → ERR_INVALID_ARG
        客户端(caller>0)  :强制只能查自己,不一致 → ERR_PERMISSION_DENY

    既不破坏内部 reader / 开局快照注入调用,又杜绝客户端读他人存档。
    """
    caller_id = pintercept.extract_player_id(context)
    if caller_id == 0:
        if req_player_id == 0:
            return 0, commonpb.ERR_INVALID_ARG
        return req_player_id, commonpb.OK
    if req_player_id != 0 and req_player_id != caller_id:
        _log_authz_deny(context, "read_other_player_denied", caller_id, req_player_id)
        return 0, commonpb.ERR_PERMISSION_DENY
    return caller_id, commonpb.OK


def _system_only(context) -> int:  # noqa: ANN001
    """系统接口鉴权:经 Envoy 的客户端(caller>0)一律拒。"""
    caller_id = pintercept.extract_player_id(context)
    if caller_id != 0:
        _log_authz_deny(context, "system_rpc_by_client", caller_id, 0)
        return commonpb.ERR_PERMISSION_DENY
    return commonpb.OK


class PlayerInternalService(pgrpc.PlayerInternalServiceServicer):
    """受 payload-bound Team 服务身份保护的 player 名称最小投影。"""

    __slots__ = ("_uc", "_verifier")

    def __init__(self, uc, verifier: internalrpcauth.Verifier | None) -> None:  # noqa: ANN001
        self._uc = uc
        self._verifier = verifier

    async def ResolvePlayerNames(self, request, context):  # noqa: N802, ANN001, ANN201
        raw_ids = [int(player_id) for player_id in request.player_ids]
        if (
            not raw_ids
            or len(raw_ids) > MAX_RESOLVE_PLAYER_NAMES
            or any(player_id <= 0 for player_id in raw_ids)
        ):
            return ppb.GetPlayerNamesResponse(code=commonpb.ERR_INVALID_ARG)
        player_ids = sorted(set(raw_ids))

        canonical = ppb.GetPlayerNamesRequest(player_ids=player_ids)
        payload = canonical.SerializeToString(deterministic=True)
        if self._verifier is None:
            plog.get().warning(
                "resolve_player_names_auth_unavailable", count=len(player_ids)
            )
            return ppb.GetPlayerNamesResponse(code=commonpb.ERR_UNAVAILABLE)
        try:
            await self._verifier.verify_with_payload(
                _internal_metadata_dict(context),
                RESOLVE_PLAYER_NAMES_METHOD,
                player_ids[0],
                payload,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            code = commonpb.ERR_PERMISSION_DENY
            if isinstance(exc, internalrpcauth.ErrUnavailable):
                code = commonpb.ERR_UNAVAILABLE
            plog.get().warning(
                "resolve_player_names_auth_rejected",
                count=len(player_ids),
                code=int(code),
                err=str(exc),
            )
            return ppb.GetPlayerNamesResponse(code=code)

        if self._uc is None:
            return ppb.GetPlayerNamesResponse(code=commonpb.ERR_UNAVAILABLE)
        try:
            names = await self._uc.get_player_names(player_ids)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                "resolve_player_names_query_failed",
                count=len(player_ids),
                err=str(exc),
            )
            return ppb.GetPlayerNamesResponse(code=errcode.as_code(exc))

        requested = set(player_ids)
        resolved: dict[int, str] = {}
        for name in names:
            player_id = int(name.player_id)
            if player_id in requested:
                resolved[player_id] = str(name.nickname)
        return ppb.GetPlayerNamesResponse(
            code=commonpb.OK,
            names=[
                ppb.PlayerName(player_id=player_id, nickname=resolved[player_id])
                for player_id in player_ids
                if player_id in resolved
            ],
        )


class PlayerService(pgrpc.PlayerServiceServicer):
    """实现 pandora.player.v1.PlayerService 的 28 个 RPC。"""

    def __init__(self, uc, ds_guard: dsauth.DSCallbackGuard | None = None) -> None:  # noqa: ANN001
        self._uc = uc
        # ds_guard 为 None 等价 mode=off(dev 行为不变)。
        self._ds_guard = ds_guard

    def set_ds_callback_guard(self, guard: dsauth.DSCallbackGuard | None) -> None:
        self._ds_guard = guard

    def _ds_gate(self, context) -> int:  # noqa: ANN001
        """DS 面两条 RPC 的令牌门。返回 0 = 放行,否则是应回的 in-band code。

        scope 与 team.GetPlayerTeam / guild.GetPlayerGuild 逐条同构:
          - require_token:全仓无内部调用方,故直连无令牌也拒,堵住「绕过 Envoy 直连业务
            端口、无标记无令牌却被当内部东西向信任」的旁路;
          - 不绑 type / pod / match_id:Hub 与 Battle DS 都在进场时查这一次,与哪台 DS、
            哪一局无关。
        """
        if self._ds_guard is None:
            return 0
        return self._ds_guard.check(context, dsauth.DSScope(require_token=True))

    # ── 档案 ──────────────────────────────────────────────────────────────

    async def GetProfile(self, request, context):  # noqa: N802
        player_id, code = _resolve_player_id(context, request.player_id)
        if code != commonpb.OK:
            return ppb.GetProfileResponse(code=code)
        try:
            profile = await self._uc.get_profile(player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            # 玩家档读不出=「卡在加载/进不了世界」的直接判据;ErrPlayerNotFound(2001)
            # 等业务码不升 access log,这里是唯一显式痕迹。EnsureProfile 成功后仍 not_found
            # 属真异常(建档丢失/分片路由错),同样靠本条暴露。
            plog.get().warning(
                "get_profile_failed", player_id=player_id, code=_code_of(exc), err=str(exc)
            )
            return ppb.GetProfileResponse(code=_code_of(exc))
        return ppb.GetProfileResponse(code=commonpb.OK, profile=profile)

    async def UpdateNickname(self, request, context):  # noqa: N802
        player_id, code = _self_player_id(context, request.player_id)
        if code != commonpb.OK:
            return ppb.UpdateNicknameResponse(code=code)
        try:
            await self._uc.update_nickname(player_id, request.nickname)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return ppb.UpdateNicknameResponse(code=_code_of(exc))
        return ppb.UpdateNicknameResponse(code=commonpb.OK)

    async def EnsureProfile(self, request, context):  # noqa: N802
        """**系统 RPC**:唯一合法调用方是 login。带玩家 JWT 一律拒 —— 否则玩家可以拿它
        给自己起任意昵称,绕过改名的全部限制。"""
        code = _system_only(context)
        if code != commonpb.OK:
            return ppb.EnsureProfileResponse(code=code)
        if request.player_id == 0:
            return ppb.EnsureProfileResponse(code=commonpb.ERR_INVALID_ARG)
        try:
            res: m.EnsureProfileResult = await self._uc.ensure_profile_named(
                request.player_id, request.nickname
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return ppb.EnsureProfileResponse(code=_code_of(exc))
        return ppb.EnsureProfileResponse(
            code=commonpb.OK,
            created=res.created,
            effective_nickname=res.nickname,
            level=res.level,
        )

    async def GetPlayerNames(self, request, context):  # noqa: N802
        """**系统 RPC + DS 令牌门**。玩家看别人的名字应该走 team / friend / guild 各自的
        成员列表,不该存在「按任意 player_id 查名字」的公开入口。"""
        code = _system_only(context)
        if code != commonpb.OK:
            return ppb.GetPlayerNamesResponse(code=code)
        if pintercept.extract_player_id(context) == 0:
            gate = self._ds_gate(context)
            if gate != 0:
                return ppb.GetPlayerNamesResponse(code=gate)
        try:
            names = await self._uc.get_player_names(list(request.player_ids))
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return ppb.GetPlayerNamesResponse(code=_code_of(exc))
        return ppb.GetPlayerNamesResponse(
            code=commonpb.OK,
            names=[ppb.PlayerName(player_id=n.player_id, nickname=n.nickname) for n in names],
        )

    # ── 英雄 ──────────────────────────────────────────────────────────────

    async def ListHeroes(self, request, context):  # noqa: N802
        player_id, code = _resolve_player_id(context, request.player_id)
        if code != commonpb.OK:
            return ppb.ListHeroesResponse(code=code)
        try:
            heroes = await self._uc.list_heroes(player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return ppb.ListHeroesResponse(code=_code_of(exc))
        return ppb.ListHeroesResponse(code=commonpb.OK, hero_ids=heroes)

    async def UnlockHero(self, request, context):  # noqa: N802
        code = _system_only(context)
        if code != commonpb.OK:
            return ppb.UnlockHeroResponse(code=code)
        if request.player_id == 0 or request.hero_id == 0:
            return ppb.UnlockHeroResponse(code=commonpb.ERR_INVALID_ARG)
        try:
            await self._uc.unlock_hero(request.player_id, request.hero_id, request.source)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return ppb.UnlockHeroResponse(code=_code_of(exc))
        return ppb.UnlockHeroResponse(code=commonpb.OK)

    # ── MMR ───────────────────────────────────────────────────────────────

    async def GetMMR(self, request, context):  # noqa: N802
        player_id, code = _resolve_player_id(context, request.player_id)
        if code != commonpb.OK:
            return ppb.GetMMRResponse(code=code)
        try:
            mmr, found = await self._uc.get_mmr(player_id, request.rating_pool)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return ppb.GetMMRResponse(code=_code_of(exc))
        # 回显归一后的池 + found:调用方靠 found 区分"没打过"与"分刚好是基线"。
        return ppb.GetMMRResponse(
            code=commonpb.OK,
            mmr=mmr,
            rating_pool=prating.normalize(request.rating_pool),
            found=found,
        )

    async def UpdateMMR(self, request, context):  # noqa: N802
        code = _system_only(context)
        if code != commonpb.OK:
            return ppb.UpdateMMRResponse(code=code)
        if request.player_id == 0:
            return ppb.UpdateMMRResponse(code=commonpb.ERR_INVALID_ARG)
        if not request.idempotency_key:
            return ppb.UpdateMMRResponse(code=commonpb.ERR_INVALID_ARG)
        try:
            new_mmr, _already = await self._uc.update_mmr(
                request.player_id,
                request.delta,
                request.reason,
                request.idempotency_key,
                request.rating_pool,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return ppb.UpdateMMRResponse(code=_code_of(exc))
        return ppb.UpdateMMRResponse(code=commonpb.OK, new_mmr=new_mmr)

    # ── 出战养成 ──────────────────────────────────────────────────────────

    async def SelectHero(self, request, context):  # noqa: N802
        player_id, code = _self_player_id(context, request.player_id)
        if code != commonpb.OK:
            return ppb.SelectHeroResponse(code=code)
        if request.hero_id == 0:
            return ppb.SelectHeroResponse(code=commonpb.ERR_INVALID_ARG)
        try:
            await self._uc.select_hero(player_id, request.hero_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return ppb.SelectHeroResponse(code=_code_of(exc))
        return ppb.SelectHeroResponse(code=commonpb.OK)

    async def GetActiveHero(self, request, context):  # noqa: N802
        player_id, code = _resolve_player_id(context, request.player_id)
        if code != commonpb.OK:
            return ppb.GetActiveHeroResponse(code=code)
        try:
            hero_id = await self._uc.get_active_hero(player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return ppb.GetActiveHeroResponse(code=_code_of(exc))
        return ppb.GetActiveHeroResponse(code=commonpb.OK, hero_id=hero_id)

    async def GrantAttributePoints(self, request, context):  # noqa: N802
        code = _system_only(context)
        if code != commonpb.OK:
            return ppb.GrantAttributePointsResponse(code=code)
        if request.player_id == 0:
            return ppb.GrantAttributePointsResponse(code=commonpb.ERR_INVALID_ARG)
        try:
            unspent = await self._uc.grant_attribute_points(
                request.player_id, request.points, request.idempotency_key
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return ppb.GrantAttributePointsResponse(code=_code_of(exc))
        return ppb.GrantAttributePointsResponse(code=commonpb.OK, unspent_points=unspent)

    async def AddExperience(self, request, context):  # noqa: N802
        """幂等入账经验并结算等级。系统 RPC:只允许后端内部直连(battle_result progress
        出箱 worker / 任务完成点 / GM),带玩家 JWT 一律拒,不在 Envoy 暴露。"""
        code = _system_only(context)
        if code != commonpb.OK:
            return ppb.AddExperienceResponse(code=code)
        if (
            request.player_id == 0
            or request.exp_delta == 0
            or not request.idempotency_key
        ):
            return ppb.AddExperienceResponse(code=commonpb.ERR_INVALID_ARG)
        try:
            state, already = await self._uc.add_experience(
                request.player_id,
                request.exp_delta,
                request.reason,
                request.idempotency_key,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return ppb.AddExperienceResponse(code=_code_of(exc))
        return ppb.AddExperienceResponse(
            code=commonpb.OK,
            level=state.level,
            exp_in_level=state.exp_in_level,
            is_max_level=state.is_max_level,
            levels_gained=state.levels_gained,
            already=already,
        )

    async def AllocateAttributePoints(self, request, context):  # noqa: N802
        player_id, code = _self_player_id(context, request.player_id)
        if code != commonpb.OK:
            return ppb.AllocateAttributePointsResponse(code=code)
        allocs = [
            m.AttrAllocation(key=a.attr_key, points=a.points) for a in request.allocations
        ]
        try:
            unspent = await self._uc.allocate_attribute_points(player_id, allocs)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return ppb.AllocateAttributePointsResponse(code=_code_of(exc))
        return ppb.AllocateAttributePointsResponse(code=commonpb.OK, unspent_points=unspent)

    async def ResetAttributes(self, request, context):  # noqa: N802
        player_id, code = _self_player_id(context, request.player_id)
        if code != commonpb.OK:
            return ppb.ResetAttributesResponse(code=code)
        try:
            unspent = await self._uc.reset_attributes(player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return ppb.ResetAttributesResponse(code=_code_of(exc))
        return ppb.ResetAttributesResponse(code=commonpb.OK, unspent_points=unspent)

    async def GetAttributes(self, request, context):  # noqa: N802
        player_id, code = _resolve_player_id(context, request.player_id)
        if code != commonpb.OK:
            return ppb.GetAttributesResponse(code=code)
        try:
            attrs, unspent = await self._uc.get_attributes(player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return ppb.GetAttributesResponse(code=_code_of(exc))
        return ppb.GetAttributesResponse(
            code=commonpb.OK,
            attributes=[
                ppb.AttributeAllocation(attr_key=a.key, points=a.points) for a in attrs
            ],
            unspent_points=unspent,
        )

    # ── 装备预设 ──────────────────────────────────────────────────────────

    async def SetEquipment(self, request, context):  # noqa: N802
        player_id, code = _self_player_id(context, request.player_id)
        if code != commonpb.OK:
            return ppb.SetEquipmentResponse(code=code)
        # identified / attributes 是 inventory 域的权威快照,**刻意不从请求信任**:
        # 只取 slot / item_config_id / instance_id 三个字段落库。
        slots = [
            m.EquipmentSlot(
                slot=e.slot, item_config_id=e.item_config_id, instance_id=e.instance_id
            )
            for e in request.equipment
        ]
        try:
            await self._uc.set_equipment(player_id, slots)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return ppb.SetEquipmentResponse(code=_code_of(exc))
        return ppb.SetEquipmentResponse(code=commonpb.OK)

    async def GetEquipment(self, request, context):  # noqa: N802
        player_id, code = _resolve_player_id(context, request.player_id)
        if code != commonpb.OK:
            return ppb.GetEquipmentResponse(code=code)
        try:
            slots = await self._uc.get_equipment(player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return ppb.GetEquipmentResponse(code=_code_of(exc))
        return ppb.GetEquipmentResponse(
            code=commonpb.OK,
            equipment=[
                ppb.LoadoutEquipment(
                    slot=s.slot, item_config_id=s.item_config_id, instance_id=s.instance_id
                )
                for s in slots
            ],
        )

    # ── 天赋 ──────────────────────────────────────────────────────────────

    async def GrantTalentPoints(self, request, context):  # noqa: N802
        code = _system_only(context)
        if code != commonpb.OK:
            return ppb.GrantTalentPointsResponse(code=code)
        if request.player_id == 0:
            return ppb.GrantTalentPointsResponse(code=commonpb.ERR_INVALID_ARG)
        try:
            unspent = await self._uc.grant_talent_points(
                request.player_id, request.points, request.idempotency_key
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return ppb.GrantTalentPointsResponse(code=_code_of(exc))
        return ppb.GrantTalentPointsResponse(code=commonpb.OK, unspent_points=unspent)

    async def SetTalents(self, request, context):  # noqa: N802
        player_id, code = _self_player_id(context, request.player_id)
        if code != commonpb.OK:
            return ppb.SetTalentsResponse(code=code)
        talents = [
            m.TalentLevel(talent_id=t.talent_id, level=t.level) for t in request.talents
        ]
        try:
            unspent = await self._uc.set_talents(player_id, talents)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return ppb.SetTalentsResponse(code=_code_of(exc))
        return ppb.SetTalentsResponse(code=commonpb.OK, unspent_points=unspent)

    async def ResetTalents(self, request, context):  # noqa: N802
        player_id, code = _self_player_id(context, request.player_id)
        if code != commonpb.OK:
            return ppb.ResetTalentsResponse(code=code)
        try:
            unspent = await self._uc.reset_talents(player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return ppb.ResetTalentsResponse(code=_code_of(exc))
        return ppb.ResetTalentsResponse(code=commonpb.OK, unspent_points=unspent)

    async def GetTalents(self, request, context):  # noqa: N802
        player_id, code = _resolve_player_id(context, request.player_id)
        if code != commonpb.OK:
            return ppb.GetTalentsResponse(code=code)
        try:
            talents, unspent = await self._uc.get_talents(player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return ppb.GetTalentsResponse(code=_code_of(exc))
        return ppb.GetTalentsResponse(
            code=commonpb.OK,
            talents=[ppb.TalentNode(talent_id=t.talent_id, level=t.level) for t in talents],
            unspent_points=unspent,
        )

    # ── 技能卡 ────────────────────────────────────────────────────────────

    async def GrantSkillCards(self, request, context):  # noqa: N802
        code = _system_only(context)
        if code != commonpb.OK:
            return ppb.GrantSkillCardsResponse(code=code)
        if request.player_id == 0:
            return ppb.GrantSkillCardsResponse(code=commonpb.ERR_INVALID_ARG)
        grants = [
            m.SkillCardGrant(card_id=g.card_id, shards=g.shards) for g in request.grants
        ]
        try:
            cards, already = await self._uc.grant_skill_cards(
                request.player_id, grants, request.idempotency_key
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return ppb.GrantSkillCardsResponse(code=_code_of(exc))
        return ppb.GrantSkillCardsResponse(
            code=commonpb.OK, cards=_to_proto_skill_cards(cards), already=already
        )

    async def UpgradeSkillCard(self, request, context):  # noqa: N802
        player_id, code = _self_player_id(context, request.player_id)
        if code != commonpb.OK:
            return ppb.UpgradeSkillCardResponse(code=code)
        try:
            card, cost = await self._uc.upgrade_skill_card(player_id, request.card_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return ppb.UpgradeSkillCardResponse(code=_code_of(exc))
        return ppb.UpgradeSkillCardResponse(
            code=commonpb.OK,
            card=ppb.SkillCard(card_id=card.card_id, level=card.level, shards=card.shards),
            shard_cost=cost,
        )

    async def SetSkillSlots(self, request, context):  # noqa: N802
        player_id, code = _self_player_id(context, request.player_id)
        if code != commonpb.OK:
            return ppb.SetSkillSlotsResponse(code=code)
        slots = [m.SkillSlot(slot=s.slot, card_id=s.card_id) for s in request.slots]
        try:
            applied = await self._uc.set_skill_slots(player_id, slots)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return ppb.SetSkillSlotsResponse(code=_code_of(exc))
        return ppb.SetSkillSlotsResponse(
            code=commonpb.OK, slots=_to_proto_skill_slots(applied)
        )

    async def GetSkillCards(self, request, context):  # noqa: N802
        player_id, code = _resolve_player_id(context, request.player_id)
        if code != commonpb.OK:
            return ppb.GetSkillCardsResponse(code=code)
        try:
            cards, slots = await self._uc.get_skill_cards(player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return ppb.GetSkillCardsResponse(code=_code_of(exc))
        return ppb.GetSkillCardsResponse(
            code=commonpb.OK,
            cards=_to_proto_skill_cards(cards),
            slots=_to_proto_skill_slots(slots),
        )

    # ── 出战快照 ──────────────────────────────────────────────────────────

    async def GetLoadout(self, request, context):  # noqa: N802
        """**本方法挂在 Envoy DS 面(:8444)**,见模块头。只在 caller==0 那一支过 DS 门。"""
        if pintercept.extract_player_id(context) == 0:
            gate = self._ds_gate(context)
            if gate != 0:
                return ppb.GetLoadoutResponse(code=gate)
        player_id, code = _resolve_player_id(context, request.player_id)
        if code != commonpb.OK:
            return ppb.GetLoadoutResponse(code=code)
        try:
            loadout = await self._uc.get_loadout(player_id)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            # DS 进场拉档失败=「玩家卡在加载界面」的第一判据;业务码不升 access log,
            # 这里是唯一显式痕迹。
            plog.get().warning(
                "get_loadout_failed", player_id=player_id, code=_code_of(exc), err=str(exc)
            )
            return ppb.GetLoadoutResponse(code=_code_of(exc))
        return ppb.GetLoadoutResponse(code=commonpb.OK, loadout=loadout)

    # ── 领奖 ──────────────────────────────────────────────────────────────

    async def ClaimReward(self, request, context):  # noqa: N802
        player_id, code = _self_player_id(context, request.player_id)
        if code != commonpb.OK:
            return ppb.ClaimRewardResponse(code=code)
        if request.reward_id == 0:
            return ppb.ClaimRewardResponse(code=commonpb.ERR_INVALID_ARG)
        try:
            await self._uc.claim_reward(
                player_id,
                request.source_type,
                request.source,
                request.activity_instance_id,
                request.reward_id,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return ppb.ClaimRewardResponse(code=_code_of(exc))
        return ppb.ClaimRewardResponse(code=commonpb.OK)

    async def GetRewardClaims(self, request, context):  # noqa: N802
        player_id, code = _resolve_player_id(context, request.player_id)
        if code != commonpb.OK:
            return ppb.GetRewardClaimsResponse(code=code)
        try:
            ids = await self._uc.get_reward_claims(
                player_id,
                request.source_type,
                request.source,
                request.activity_instance_id,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return ppb.GetRewardClaimsResponse(code=_code_of(exc))
        return ppb.GetRewardClaimsResponse(code=commonpb.OK, claimed_reward_ids=ids)


def _to_proto_skill_cards(cards) -> list:  # noqa: ANN001
    return [
        ppb.SkillCard(card_id=c.card_id, level=c.level, shards=c.shards) for c in cards
    ]


def _to_proto_skill_slots(slots) -> list:  # noqa: ANN001
    return [ppb.SkillSlot(slot=s.slot, card_id=s.card_id) for s in slots]
