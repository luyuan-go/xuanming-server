"""Hub 放行门的三态回归 —— 盯死 `inspect_battle_route` 的**返回形状**。

★ 2026-08-24 实测:「打完一局 → 结算 → 回大厅」这条主路径在 Python 栈上**结构性失效**。

  Go 的 `InspectBattleRoute` 返回 `(state, err)`;Python 移植改成了「成功回单个
  `BattleRouteState`、失败抛异常」(battleroute.py / biz.py 的 `inspect_battle_route`),
  但两个调用点 —— `_try_battle_reconnect` 与 `_guard_hub_route_against_active_battle`
  —— 都保留了 Go 的双值解包 `state, route_err = await ...`。`BattleRouteState` 是
  `IntEnum`、不可迭代,于是**只要走到那一行就必抛 TypeError**,且不产生任何日志:

      LogPandoraLogin: Warning: IssueDSTicket failed: player_id=... grpc=0 code=1
                                has_ticket=0 err=

  err 是空的、login 侧只有一条 `rpc_inband_error code=1`,连哪个分支拒的都看不出来。
  真正的后果是下面这条 `TERMINAL → 放行 Hub` 分支**从来没有被执行过**:玩家结算后
  只能退到权威路由重查,而那条路又被「结算后 owner 未释放」堵死(见
  test_ds_allocator_biz_heartbeat 的 `..._releases_owner_on_the_same_beat`)
  ⇒ 连查 3 次 → authority_entry_terminal → 被踢回登录。

  三条用例都写成「把 try/except 折回改回双值解包就会红」的形状,并且覆盖三态各自的
  归宿,防止有人为了"修 TypeError"顺手把 UNKNOWN 也放行(那是 P0:对局还活着却放进
  Hub = 双在场)。
"""

from __future__ import annotations

import pytest

from pandorapy import errcode
from pandorapy.services.login import battleroute as lbattleroute
from pandorapy.services.login import biz as lbiz
from pandorapy.services.login import clients as lclients

PLAYER = 25306097631920129
MATCH = 27533240038359040


class _Issuer:
    """`inspect_battle_route` 的替身 —— 严格照搬**真实签名**:回单值 / 抛异常。

    绝不在这里回 `(state, err)` 元组:那样会把被测的形状不匹配一起伪造掉,
    用例就再也钉不住任何东西了。
    """

    def __init__(self, state=None, exc: BaseException | None = None) -> None:
        self._state = state
        self._exc = exc
        self.calls: list[tuple[int, int]] = []

    async def inspect_battle_route(self, player_id: int, match_id: int):
        self.calls.append((player_id, match_id))
        if self._exc is not None:
            raise self._exc
        return self._state


def _uc(issuer: _Issuer) -> lbiz.LoginUsecase:
    """只装配放行门用得到的字段。`LoginUsecase` 无 __slots__,可直接 __new__。"""
    uc = lbiz.LoginUsecase.__new__(lbiz.LoginUsecase)
    uc._notifier = object()          # 非 None 即可:只用于"locator 配了没"
    uc._battle_ticket_issuer = issuer
    uc._match_resolver = None
    # legacy dev 裸跑档(两轴皆关)—— 与出事那台机器一致。
    uc._require_hub_assignment_binding = False
    uc._rs256_ds_ticket_profile = False

    async def _in_battle(_player_id: int) -> lclients.BattleLocation:
        # presence 投影滞后是常态:对局已结算,BATTLE 位置键还没过 TTL。
        return lclients.BattleLocation(
            in_battle=True,
            match_id=MATCH,
            battle_addr="192.168.2.28:7800",
            presence_state="LOCATION_STATE_BATTLE",
        )

    uc._query_battle_location = _in_battle  # type: ignore[method-assign]
    return uc


@pytest.mark.asyncio
async def test_terminal_battle_allows_hub_with_source_fence() -> None:
    """对局已终态 → 放行 Hub,并把 source_match_id 作为 fence 返回。

    这就是「结算完回大厅」的主路径。★ 变异:把调用点改回
    `state, route_err = await ...inspect_battle_route(...)` → 本条红(TypeError)。
    """
    issuer = _Issuer(state=lbattleroute.BattleRouteState.TERMINAL)
    uc = _uc(issuer)

    fence = await uc._guard_hub_route_against_active_battle(PLAYER)

    assert fence == MATCH, "fence 必须来自路由权威,不是客户端自报"
    assert issuer.calls == [(PLAYER, MATCH)]


@pytest.mark.asyncio
async def test_active_battle_still_rejected() -> None:
    """对局仍 live → 拒发 Hub 票(ErrInvalidState),让玩家走重连。

    修 TypeError 不许顺手放宽这道门。★ 变异:把 ACTIVE 也放行 → 本条红(双在场)。
    """
    uc = _uc(_Issuer(state=lbattleroute.BattleRouteState.ACTIVE))

    with pytest.raises(errcode.PandoraError) as ei:
        await uc._guard_hub_route_against_active_battle(PLAYER)
    assert errcode.as_code(ei.value) == errcode.ErrInvalidState


@pytest.mark.asyncio
async def test_route_authority_failure_is_unknown_not_crash() -> None:
    """权威查询抛错 → 按 Go 语义折回 (UNKNOWN, err) → 可重试的 ErrUnavailable。

    关键是**不能**变成裸异常:裸异常在 gRPC 上表现为 code=1 且 err 为空,
    运维看不出是哪道门拒的(2026-08-24 排查时就卡在这里)。
    ★ 变异:去掉 try/except 折回 → 本条红(抛出的是 TypeError/RuntimeError 而非 ErrUnavailable)。
    """
    uc = _uc(_Issuer(exc=errcode.PandoraError(errcode.ErrUnavailable, "redis down")))

    with pytest.raises(errcode.PandoraError) as ei:
        await uc._guard_hub_route_against_active_battle(PLAYER)
    assert errcode.as_code(ei.value) == errcode.ErrUnavailable


@pytest.mark.asyncio
async def test_unknown_state_is_rejected() -> None:
    """显式 UNKNOWN(零值)同样 fail-closed —— 绝不折叠成终态放行。"""
    uc = _uc(_Issuer(state=lbattleroute.BattleRouteState.UNKNOWN))

    with pytest.raises(errcode.PandoraError) as ei:
        await uc._guard_hub_route_against_active_battle(PLAYER)
    assert errcode.as_code(ei.value) == errcode.ErrUnavailable
