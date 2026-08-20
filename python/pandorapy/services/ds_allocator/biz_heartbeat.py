"""ds_allocator 业务层**心跳**(Model B 授权心跳 + legacy 心跳)—— 对应 Go 侧
`services/battle/ds_allocator/internal/biz/allocator.go` 第 2426–3358 行。

覆盖的 Go 函数(逐个对应,顺序一致):

    HeartbeatResult(struct)
    RedisAuthorityEnabled / HeartbeatAuthorized / HeartbeatAuthorizedWithPlayers
    legacyPodUIDPreflightCredentialMatches
    Heartbeat / HeartbeatWithCensus / localCredentialACK / heartbeatLegacy
    resolveRosterJoinModeSafe / effectiveRosterJoinDeadline / rosterAbsentees
    recordNoShowPenalties / finishEmptyAbandon / refreshBattleLocations
    ListBattles

最终组装(见同目录 `biz.py`):

    class AllocatorUsecase(SweepMixin, HeartbeatMixin, ReleaseMixin,
                           AllocateMixin, AllocatorUsecaseBase): ...

═══════════════════════════════════════════════════════════════════════════════
这批的失败形状:**授权判定被"看着差不多"地简化**
═══════════════════════════════════════════════════════════════════════════════

`HeartbeatAuthorizedWithPlayers` 是 Battle DS 写权限的**授权点**(CLAUDE.md §9.6
五要件:①身份可验证 ②owner 授权 ③fencing ④额度 ⑤审计)。它同时是:

  - **§9.22 的 owner 屏障执行点**:`activate_heartbeat` 是 pending→active 提升、
    服务端心跳时刻、battle 投影三者的**唯一线性化点**。它拒绝一跳,DS 侧就必须按
    §9.22 自我 fencing 停止处理输入 —— 换句话说,这里每多放行一条不该放行的心跳,
    就多一次"两台 DS 同时认为自己有权改同一个玩家"的机会。
  - **§9.19 玩家不卡死链上唯一的后端驱动**:驱逐单(`EvictionOrders`)是"玩家点了
    退出副本"这条链上**唯一**从后端流向 DS 的指令。这里少发一次,玩家就卡在
    「退出副本」上直到 DS 自己超时。

因此本文件里**没有**任何一条判定被合并、被提前 return、被"反正后面还会再判一次"
地省掉。几处看起来冗余的重复判空(`out.battle is not None` 在同一函数里出现四次)
都对应 Go 的四个独立 `if`,顺序与短路行为逐条一致。

═══════════════════════════════════════════════════════════════════════════════
与 Go 的必要形变(其余逐行同构)
═══════════════════════════════════════════════════════════════════════════════

  1. **`(值, error)` → 返回值 + 抛异常**。Go 的 `HeartbeatAuthorizedWithPlayers`
     失败时返回 `(nil, err)`,调用方只看 err;Python 直接抛。
     `localCredentialACK` 的 `(identity, ok)` 在 Python 侧是
     `BattleCredentialIdentity | None`(与 `local_allocator.local_credential_ack`
     已落地的签名一致,不另造一个 `(值, bool)` 元组)。

  2. **哨兵 `error` → 异常类**。`errHeartbeatTerminal` / `errHeartbeatPodMismatch` /
     `errHeartbeatAllocationFenced` 在 Go 里是从乐观锁回调 `return` 出来、再用
     `errors.Is` 比对的值;Python 用 `biz_base` 已定义的三个异常类,
     `raise` / `except` 与 `errors.Is` 同语义。
     ★ `except` 顺序与 Go 的 `switch { case errors.Is(...) }` **逐格一致**,
       `errcode.as_code(err) == ErrDSPodNotFound` 排在三个哨兵之后(Go 的第四个 case)。

  3. **Duration 一律以「秒」为单位并在名字里写出来**(`battle_ttl_sec()`),毫秒量
     用 `_duration_ms()` 做**向零截断**,与 Go 的 `Duration.Milliseconds()` 逐位一致。
     写成 `int(total_seconds() * 1000)` 会在 `2.5h` 这类值上被浮点尾数咬掉 1ms。

  4. **`u == nil` 的接收者判空删掉**。Go 的 `RedisAuthorityEnabled` 首行判了
     `u != nil`(Go 允许 nil 指针调用方法);Python 里 `self` 不可能是 None,
     保留只会写出一行永远为真的死代码(§15.5,与基座同一处理)。

  5. **proto message 字段的"未设置"必须用 `HasField`**。Go 的 `auth.GetActive()`
     在未设置时返回 `nil`,`matches(nil)` 恒 false;Python 的 `auth.active` **永远**
     返回一个零值消息,`c is not None` 恒真。因此
     `legacy_pod_uid_preflight_credential_matches` 复用 `battle_auth._active_of` /
     `_pending_of`(它们已经用 `HasField` 做了 nil-safe 取值)。

     ★ 诚实说明(变异实测结论,不要照抄直觉):就**当前这组判据**而言,写成
       `snapshot.auth.active` 也不会真的放行 —— 零值凭据要与一个零值身份全等,而零值
       身份的 `exp_ms == 0` 已经先被 `ident.exp_ms <= now_ms()` 那道过期门拦下。
       坚持用 `_active_of` 的理由不是"现在会出 bug",而是**不把本函数的正确性寄存在
       另一道门恰好还在**:过期门将来若因任何原因放宽(允许 leeway、调换判定顺序、
       接受 exp_ms=0 表示"不过期"),这里就是唯一防线;且与 Go 同构的写法让 review
       不必每次重新推导一遍"为什么零值不会全等"。

  6. **`refresh_battle_locations` 不 await**。Go 侧它是 fire-and-forget(内部 `go func`
     + `plog.Detach` + 短超时),同步返回;Python 侧同构为**同步方法**(与
     `AllocatorUsecaseBase.kill_stranded_ds` 一致),故这里不加 `await`。

  7. **`census` 相关的 list 一律在传出前 `list(...)` 拷贝**。Go 的 `[]uint64` 传的是
     切片头,调用方复用底层数组是 Go 侧已知的坑;Python 侧把 proto 的
     `RepeatedScalarContainer` 直接传给下游会让下游拿到一个**活的**视图 ——
     下一次 CAS 重跑改了 `b.player_ids`,已经传出去的那份也跟着变。

═══════════════════════════════════════════════════════════════════════════════
Go 第 3071 行之后的七个函数(本文件后半段,契约与调用点逐字对应)
═══════════════════════════════════════════════════════════════════════════════

    self.resolve_roster_join_mode_safe() -> str
        Go: `resolveRosterJoinModeSafe`。配置非法时滑向 `conf.ROSTER_JOIN_MODE_OBSERVE`。

    self.effective_roster_join_deadline() -> datetime.timedelta
        Go: `effectiveRosterJoinDeadline`。off 档归零。**返回 timedelta 而非秒**:
        本文件两处调用一处要秒(喂 `BattleHeartbeatInput.roster_join_deadline_sec`)、
        一处要毫秒(legacy 的 `roster_deadline_ms`),用 timedelta 才能两处都不二次换算。

    roster_absentees(roster, census) -> list[int]  (模块级 + 类内 staticmethod 绑定)
        Go: 包级函数 `rosterAbsentees`。保持 roster 原序;`census` 缺席时**不**兜底成
        "全员缺席"(那会把每一局都判弃)。

    await self.record_no_show_penalties(match_id, player_ids) -> None
        Go: `recordNoShowPenalties`。fail-open,幂等性来自唯一调用点的 CAS 标记。

    await self.finish_empty_abandon(
        match_id, pod_name, instance_uid, pod_uid, allocation_id, release_track,
        instance_epoch, player_ids, map_id, game_mode,
        no_show, roster_incomplete, absentees) -> HeartbeatResult
        Go: `finishEmptyAbandon`。回收 pod + 投递 lifecycle + 移出 active,全路径回 stop。

    self.refresh_battle_locations(player_ids, match_id, ds_addr) -> None
        Go: `refreshBattleLocations`。**同步** fire-and-forget(见形变 6)。

    await self.list_battles(state_filter) -> list[dspb.BattleInfo]
        Go: `ListBattles`。本文件不调用(gm / service 层入口)。

★ Go 的 `RunHeartbeatSweep` / `SweepWriterLease` / `SetSweepWriterLease`(3358 行之后)
  **不在本文件**:它们随 sweep 一起落在 `biz_sweep.py` / `biz_base.py`,别在这里再造一份。
"""

from __future__ import annotations

import asyncio
import dataclasses
import datetime as _dt
from typing import Any

from pandora.ds.v1 import allocator_pb2 as dspb

from pandorapy import errcode, godur, placement, safego
from pandorapy import log as plog
from pandorapy.services.ds_allocator import conf as dsconf
from pandorapy.services.ds_allocator.agones_allocator import (
    AuthoritativeGameServerAllocation,
)
from pandorapy.services.ds_allocator.battle_auth import (
    BattleAuthoritySnapshot,
    BattleCredentialIdentity,
    BattleExpectedInstance,
    BattleHeartbeatInput,
    _active_of,
    _pending_of,
    _str_eq,
    now_ms,
    roster_gate_armable,
)
from pandorapy.services.ds_allocator.biz_base import (
    COMMAND_NONE,
    COMMAND_STOP,
    LOCATION_REFRESH_TIMEOUT_SEC,
    REASON_ALLOC_MATCH_ID_REQUIRED,
    REASON_HEARTBEAT_ACTIVATE_REJECTED,
    REASON_HEARTBEAT_AUTHORITY_READ,
    REASON_HEARTBEAT_CENSUS_INCONSISTENT,
    REASON_HEARTBEAT_DEPARTURE_RECONCILE,
    REASON_HEARTBEAT_MODEL_B_OFF,
    REASON_HEARTBEAT_OWNER_LEASE,
    REASON_HEARTBEAT_UPDATE_FAILED,
    REASON_POD_UID_RESOLVE_FAILED,
    REASON_STOP_BATTLE_ABANDONED,
    REASON_STOP_BATTLE_ENDED,
    REASON_STOP_BATTLE_MISSING,
    REASON_STOP_TERMINAL_AUTH,
    STATE_ABANDONED,
    STATE_ALLOCATION_ABORT,
    STATE_ALLOCATION_EMPTY_FENCE,
    STATE_ALLOCATION_RECONCILING,
    STATE_ALLOCATION_UNCERTAIN,
    STATE_ENDED,
    STATE_PREACTIVE_RELEASING,
    STATE_READY,
    STATE_RUNNING,
    STATE_WARMING,
    UPDATE_MAX_RETRY,
    HeartbeatAllocationFencedError,
    HeartbeatPodMismatchError,
    HeartbeatTerminalError,
)
from pandorapy.services.ds_allocator.biz_release import OWNER_RELEASE_BUDGET_SEC
from pandorapy.services.ds_allocator.departure import BattleDepartureSource
from pandorapy.services.ds_allocator.gameserver import LocalBattleCredentialSource
from pandorapy.services.ds_allocator.owner_authority import (
    OWNER_TYPE_BATTLE,
    owner_admit_census_weak,
    owner_release_abandoned_players_weak,
)
from pandorapy.services.ds_allocator.owner_lease import renew_owner_lease_gate

__all__ = [
    "LOCATION_REFRESH_TASK_NAME",
    "NO_SHOW_PENALTY_MAX_SHIFT",
    "OWNER_ADMIT_BUDGET_SEC",
    "HeartbeatMixin",
    "HeartbeatResult",
    "legacy_pod_uid_preflight_credential_matches",
    "roster_absentees",
]

#: 心跳路径上 owner 代提交 Admit / owner 释放的独立预算(秒)。
#: Go 侧两处都是字面量 `2*time.Second`;释放侧复用 `biz_release.OWNER_RELEASE_BUDGET_SEC`
#: (同为 2.0),这里只为 Admit 侧留一个具名常量 —— 心跳是 5s 一跳的高频路径,
#: 预算写成字面量时"为什么是 2 秒"会随第一次调参一起消失。
OWNER_ADMIT_BUDGET_SEC = 2.0

#: fire-and-forget 的 BATTLE 位置续期任务名(`safego.spawn` 要求具名,禁止裸 task)。
#: 与 `biz_base.KILL_STRANDED_TASK_NAME` / `biz_sweep.SWEEP_TASK_NAME` 同一命名族:
#: 任务在停机日志 / 未捕获异常里只剩这个字符串,写成 lambda 就再也认不出主人。
LOCATION_REFRESH_TASK_NAME = "ds_refresh_battle_locations"

#: no-show 退避指数移位的硬上限。对应 Go 的 `if shift > 8 { shift = 8 }`。
#: 30s << 8 = 128min,远超任何合理 cap,再大没有意义 —— 它同时是**溢出闸**:
#: Go 那边 `base << shift` 在 shift 过大时会把 int64 纳秒直接移成负数,
#: Python 的 int 虽无上限,但 `timedelta` 超过 ~2.7e6 天会抛 OverflowError,
#: 一条背压日志因此变成判弃收尾路径上的异常。两边都必须钳。
NO_SHOW_PENALTY_MAX_SHIFT = 8


def _duration_ms(td: _dt.timedelta) -> int:
    """`timedelta` → 毫秒(**向零截断**)。等价于 Go 的 `Duration.Milliseconds()`。"""
    return int(td / _dt.timedelta(milliseconds=1))


# ── RPC 3:Heartbeat 的出参 ──────────────────────────────────────────────────


@dataclasses.dataclass(slots=True)
class HeartbeatResult:
    """Heartbeat 的出参(下发给 DS 的控制指令)。对应 Go 的 `HeartbeatResult`。

    `accepted_*` 五项是**心跳 ACK**:UE 的 `SendBattleHeartbeat` 用
    `IsBoundToRequest` 逐字段比对(uid / instance_epoch / gen / jti / writer_epoch),
    任一项不等就把 `command` 与 `eviction_orders` **整份清空并置错**。所以这五项
    要么全填真值,要么一个都不填 —— 半截 ACK 比不回 ACK 更难查。

    ★ `eviction_orders` 用 `default_factory=list` 而不是共享的 `[]` 字面量:
      dataclass 的可变默认值在 Python 里会被所有实例共享(这里 `slots=True` +
      `field` 已经拦住了,写清楚是免得后来人"简化"回去)。
    """

    command: str = ""
    accepted_token_gen: int = 0  # Go: uint64
    accepted_token_jti: str = ""
    accepted_instance_uid: str = ""
    accepted_instance_epoch: int = 0  # Go: uint32
    accepted_writer_epoch: int = 0  # Go: uint32
    #: list[dspb.BattleEvictionOrder]
    eviction_orders: list[Any] = dataclasses.field(default_factory=list)


def legacy_pod_uid_preflight_credential_matches(
    snapshot: BattleAuthoritySnapshot,
    ident: BattleCredentialIdentity,
) -> bool:
    """滚动升级遗留记录(无 pod_uid)回填前的**凭据全等**前置门。
    对应 Go 的 `legacyPodUIDPreflightCredentialMatches`。

    为什么这道门必须在回填之前:`ensure_durable_release_pod_uid` 会把一个 exact
    Pod UID 写进权威记录,而那正是后续所有精确回收(DELETE GameServer)的依据。
    上报方不是当前被授权实例时回填,等于让一台**未经授权**的 DS 指定"该删哪个 Pod"。

    判据逐条照抄,一个都不合并:

      auth 存在 → auth/battle 同 match_id、同 allocation_id
      → auth 的 pod/uid/epoch 与上报身份全等 → 上报凭据**未过期**
      → 且上报身份与 auth 的 active 或 pending 凭据**八项全等**
        (gen / jti / exp_ms / kid / instance_uid / instance_epoch /
         token_sha256 / writer_epoch)

    ★ `snapshot.auth_found` 与 `snapshot.auth is None` 都要判(Go 是
      `!snapshot.AuthFound || snapshot.Auth == nil`):found 为真但 auth 为 None
      是数据层的不变量违反,静默当成"匹配"会直接放行回填。
    ★ 凭据串比较走 `battle_auth._str_eq`(`hmac.compare_digest`):jti / kid /
      token_sha256 是"持有即可冒充"的凭据标识,朴素 `==` 在首个不同字节处短路,
      给远程攻击者一个逐字节爆破的时序侧信道。
    """
    if (
        not snapshot.auth_found
        or snapshot.auth is None
        or snapshot.battle is None
        or snapshot.auth.match_id != snapshot.battle.match_id
        or snapshot.auth.allocation_id != snapshot.battle.allocation_id
        or snapshot.auth.ds_pod_name != ident.pod_name
        or snapshot.auth.instance_uid != ident.instance_uid
        or snapshot.auth.instance_epoch != ident.instance_epoch
        or ident.exp_ms <= now_ms()
    ):
        return False

    def matches(cred) -> bool:  # noqa: ANN001 —— dspb.BattleDSCredential | None
        return (
            cred is not None
            and cred.gen == ident.gen
            and _str_eq(cred.jti, ident.jti)
            and cred.exp_ms == ident.exp_ms
            and _str_eq(cred.kid, ident.kid)
            and cred.instance_uid == ident.instance_uid
            and cred.instance_epoch == ident.instance_epoch
            and _str_eq(cred.token_sha256, ident.token_sha256)
            and cred.writer_epoch == ident.writer_epoch
        )

    # ★ `_active_of` / `_pending_of` 用 `HasField` 做 nil-safe 取值,对应 Go 的
    #   `GetActive()` / `GetPending()` 返回 nil。当前判据下过期门已先挡住零身份,
    #   所以这不是唯一防线,但也不得因此改写成 `snapshot.auth.active`(见模块头「形变 5」)。
    return matches(_active_of(snapshot.auth)) or matches(_pending_of(snapshot.auth))


def roster_absentees(roster: list[int], census: list[int]) -> list[int]:
    """roster 里没出现在 census 中的玩家(保持 roster 原序,便于日志比对)。
    对应 Go 的包级函数 `rosterAbsentees`。

    **输入语义严格**:`census` 必须是 DS 上报的**真实在场名单**,不是 roster 也不是
    数量。拿数量判只能得到「少了几个」,说不出少的是谁 —— 而「只罚缺席者、不罚在场的
    人」恰恰要求点名到人。

    ★ 调用方负责保证 census 确实存在(`snapshot_present`),本函数**不替它兜底**:
      census 缺席时返回「全员缺席」会把每一局都判弃。这就是为什么这里没有
      `if not census: return list(roster)` 这种看着"更健壮"的分支。

    ★ `roster` 为空返回 `[]`(Go 返回 `nil`):两者在唯一的消费形状 `len(...) == 0`
      与 `for` 遍历下逐字等价,不必为此造一个 `None`。
    """
    if len(roster) == 0:
        return []
    present = set(census)
    return [pid for pid in roster if pid not in present]


class HeartbeatMixin:
    """DS 心跳(Model B 授权心跳 + legacy 心跳)。

    ★ **不继承任何东西**:字段(`self.model_b` / `self.auth_repo` / `self.repo` /
      `self.cfg` / `self.owner_lease` / `self.owner_auth` / `self.owner_admitted` /
      `self.locator` / `self.alloc` / `self.ds_credential_ttl_sec`)与 helper
      (`battle_ttl_sec` / `kill_stranded_ds` / `ensure_durable_release_pod_uid`)
      全部来自 `AllocatorUsecaseBase` 与其它 mixin,在 `biz.AllocatorUsecase` 组装后
      才齐备。单独实例化本类调用任一方法都会 `AttributeError` —— 这是刻意的:
      比起放一堆 `raise NotImplementedError` 的假实现,让组装缺件在**第一次调用**
      就带着属性名炸出来更容易定位。
    """

    # ── Model B:授权心跳 ──────────────────────────────────────────────────

    def redis_authority_enabled(self) -> bool:
        """供 service / gm 选择严格 Model B 路径。对应 Go 的 `RedisAuthorityEnabled`。

        开启后**不存在** legacy fallback:`service` 层据此二选一,不是"先试 Model B
        失败再退 legacy"。半开启状态下 `resolve_battle_target` 会返回 `ErrUnavailable`,
        玩家重连被静默退化成回大厅,而运维看到的是"服务健康"。
        """
        return self.model_b

    async def heartbeat_authorized(
        self,
        match_id: int,
        ident: BattleCredentialIdentity,
        player_count: int,
        state: str,
        ts_ms: int,
    ) -> HeartbeatResult:
        """Model B 唯一心跳入口。对应 Go 的 `HeartbeatAuthorized`。

        pending 激活、active 幂等续命、battle 投影与服务端接收时间在 Redis 同槽一次
        EXEC 完成;请求 `ts_ms` **不参与任何授权 / TTL 判断**(一个未来时间戳能让
        失联 DS 长期"心跳新鲜")。
        """
        return await self.heartbeat_authorized_with_players(
            match_id, ident, player_count, state, ts_ms, False, 0, "", None, None
        )

    async def heartbeat_authorized_with_players(  # noqa: C901, PLR0912, PLR0913, PLR0915 —— 与 Go 逐条对应
        self,
        match_id: int,
        ident: BattleCredentialIdentity,
        player_count: int,
        state: str,
        _ts_ms: int,
        snapshot_present: bool,
        census_capability_version: int,
        census_id: str,
        active_player_ids: list[int] | None,
        acknowledged_departure_ids: list[str] | None,
    ) -> HeartbeatResult:
        """Battle→Hub 物理离场闭环心跳。对应 Go 的 `HeartbeatAuthorizedWithPlayers`。

        只有 `snapshot_present=True` 的新 DS 才能用完整可信 active-player 快照提交
        departure;旧 DS 的 proto3 零值始终 fail-closed,但仍可收到 order 等待升级 /
        source UID teardown。

        ★ 第五个形参 `_ts_ms` 刻意保留但**完全不用**(Go 的形参名就是 `_`):
          删掉它会改变 service 层的调用形状,而"客户端时间只能在仓外做遥测"这条
          纪律要靠"参数在、但没有任何一处读它"来自证。

        Raises:
            errcode.PandoraError(ErrInvalidState): 本副本未启用 Redis 权威。
            errcode.PandoraError(ErrInvalidArg): DS 自报在场人数超过它给出的完整名单。
            其它: 权威读 / 激活 / owner 租约 / 离场对账的错误**原样上抛**
                (DS 据此按 §9.22 自我 fencing)。
        """
        # Go 的 `[]uint64` nil 与空切片同义;Python 侧把 None 归一成空 list,
        # 后面所有 `len(...)` / 遍历就不必各自判空。
        census: list[int] = list(active_player_ids) if active_player_ids else []
        acked: list[str] = (
            list(acknowledged_departure_ids) if acknowledged_departure_ids else []
        )

        if not self.model_b:
            plog.get().warning(
                "battle_heartbeat_refused",
                reason=REASON_HEARTBEAT_MODEL_B_OFF,
                match_id=match_id,
                pod=ident.pod_name,
            )
            raise errcode.PandoraError(
                errcode.ErrInvalidState, "battle Redis authority is not enabled"
            )
        if snapshot_present and player_count > len(census):
            plog.get().warning(
                "battle_heartbeat_refused",
                reason=REASON_HEARTBEAT_CENSUS_INCONSISTENT,
                match_id=match_id,
                pod=ident.pod_name,
                player_count=player_count,
                census=len(census),
                hint="DS 自报在场人数比它给出的完整名单还多,名单不可信,整跳拒绝",
            )
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "battle heartbeat player_count=%d exceeds complete owner census=%d",
                player_count,
                len(census),
            )

        # 滚动升级期的 Model-B writer 可能遇到一条**在 pod_uid 字段存在之前**写下的
        # active 记录。必须在首次心跳、且在 activate_heartbeat 有机会把一局空场原子
        # 转成 ABANDONED 并让其授权永久化**之前**回填。K8s 对象缺失 / 已重建时本跳
        # 以零 Redis 状态转移被拒。
        try:
            preflight = await self.auth_repo.read_authority(match_id)
        except asyncio.CancelledError:
            # ★ 必须紧邻宽 except 之上:取消是停机控制流,不是"权威读失败";
            #   吞掉它会让优雅排空期这条心跳路径不退出。
            raise
        except BaseException as exc:
            plog.get().warning(
                "battle_heartbeat_refused",
                reason=REASON_HEARTBEAT_AUTHORITY_READ,
                match_id=match_id,
                pod=ident.pod_name,
                err=str(exc),
                hint="Redis 权威读失败,本跳零状态转移;连续失败会走满 15s 心跳超时被判弃",
            )
            raise
        if (
            preflight.battle_found
            and preflight.battle is not None
            and preflight.battle.pod_uid == ""
            and preflight.battle.allocation_id != ""
            and preflight.battle.ds_pod_name == ident.pod_name
            and preflight.battle.gameserver_uid == ident.instance_uid
            and preflight.battle.instance_epoch == ident.instance_epoch
            and legacy_pod_uid_preflight_credential_matches(preflight, ident)
        ):
            try:
                await self.ensure_durable_release_pod_uid(
                    match_id,
                    ident.pod_name,
                    BattleExpectedInstance(
                        allocation_id=preflight.battle.allocation_id,
                        instance_uid=ident.instance_uid,
                        instance_epoch=ident.instance_epoch,
                    ),
                    preflight.battle.release_track,
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                plog.get().warning(
                    "battle_heartbeat_refused",
                    reason=REASON_POD_UID_RESOLVE_FAILED,
                    match_id=match_id,
                    pod=ident.pod_name,
                    uid=ident.instance_uid,
                    epoch=ident.instance_epoch,
                    err=str(exc),
                    hint=(
                        "滚动升级遗留的无 pod_uid 记录回填失败,本跳零状态转移;"
                        "详因见同 trace_id 的 battle_pod_uid_preflight_refused"
                    ),
                )
                raise

        try:
            out = await self.auth_repo.activate_heartbeat(
                match_id,
                ident,
                BattleHeartbeatInput(
                    player_count=player_count,
                    state=state,
                    auth_ttl_sec=self.ds_credential_ttl_sec,
                    battle_ttl_sec=self.battle_ttl_sec(),
                    empty_battle_timeout_sec=(
                        self.cfg.empty_battle_timeout_td().total_seconds()
                    ),
                    no_show_timeout_sec=self.cfg.resolve_no_show_timeout().total_seconds(),
                    stability_beats=self.cfg.activation_stability_beats,
                    stability_span_ms=_duration_ms(self.cfg.activation_stability_span_td()),
                    # 花名册到齐期限(INC-20260813-001)。census 未上报时 data 层整道
                    # 跳过 —— 拿 player_count 硬猜会把每一局都判成缺员。
                    roster_join_deadline_sec=(
                        self.effective_roster_join_deadline().total_seconds()
                    ),
                    roster_join_arm_window_sec=(
                        self.cfg.resolve_roster_join_arm_window().total_seconds()
                    ),
                    census_present=snapshot_present,
                    active_player_ids=tuple(census),
                    # observe→enforce 激活档与策略代
                    # (共享谓词 conf.roster_deadline_should_abandon)。
                    roster_join_mode=self.resolve_roster_join_mode_safe(),
                    roster_policy_generation=self.cfg.roster_policy_generation,
                ),
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            # 这是 Model B 心跳唯一的授权/fencing 判定点,原来失败**一条日志都没有**:
            # BattleAuthStaleError 是 ErrUnauthorized,access log 只落 rpc_ok=DEBUG。
            # 「DS 在跑但后端认为它没心跳」这类故障以前在后端完全查不出来。
            # 具体是哪一条 fencing 规则拒的,见 data 层同 trace_id 的 battle_ds_auth_rejected。
            plog.get().warning(
                "battle_heartbeat_refused",
                reason=REASON_HEARTBEAT_ACTIVATE_REJECTED,
                match_id=match_id,
                pod=ident.pod_name,
                uid=ident.instance_uid,
                epoch=ident.instance_epoch,
                gen=ident.gen,
                writer_epoch=ident.writer_epoch,
                state=state,
                player_count=player_count,
                code=errcode.as_code(exc),
                err=str(exc),
            )
            raise

        if out.roster_would_abandon:
            # observe 采证 / 代不匹配豁免(与 legacy 路径同一条 WARN,
            # enforce 翻闸前的唯一现场依据)。
            plog.get().warning(
                "roster_incomplete_would_abandon",
                match_id=match_id,
                pod=ident.pod_name,
                mode=self.resolve_roster_join_mode_safe(),
                config_policy_generation=self.cfg.roster_policy_generation,
                deadline_ms=_duration_ms(self.cfg.resolve_roster_join_deadline()),
                authority="redis",
                hint="mode=enforce 且 battle 代==配置代 才会真判弃;见 decision-revisit §5",
            )
        if out.activation_pending:
            # 两阶段激活(INC-20260727-001 第三 P0):稳定性证据不足,不发 ACK、
            # 不下发指令、不续 owner 租约(实例尚未服务)、不做离场对账;DS 每 tick
            # 幂等重试 staged 心跳,wait_battle_ready 因 auth 仍 BOOTSTRAP 不会放行 ds_addr。
            plog.get().debug(
                "battle_ds_activation_pending",
                match_id=match_id,
                pod=ident.pod_name,
                gen=ident.gen,
            )
            return HeartbeatResult()

        # owner 权威实例租约双写(owner-authority.md migrate ⑥):必须在心跳响应
        # 返回前完成,失败语义(弱/强依赖)见 renew_owner_lease_gate。
        owner_lease_track = out.battle.release_track if out.battle is not None else ""
        try:
            await renew_owner_lease_gate(
                self.owner_lease,
                self.owner_lease_required,
                ident.pod_name,
                ident.instance_uid,
                ident.instance_epoch,
                owner_lease_track,
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            # 只有 required(contract 强依赖)档会走到这里;弱依赖档由
            # renew_owner_lease_gate 内部按窗口限流 Warn 后放行。强依赖失败 =
            # 心跳整跳失败 = DS 会自我 fencing 停玩,是"玩家突然被踢出副本"的
            # 直接原因,必须显式留证。
            plog.get().warning(
                "battle_heartbeat_refused",
                reason=REASON_HEARTBEAT_OWNER_LEASE,
                match_id=match_id,
                pod=ident.pod_name,
                uid=ident.instance_uid,
                epoch=ident.instance_epoch,
                release_track=owner_lease_track,
                err=str(exc),
                hint="owner 权威租约续写是强依赖档;DS 拿不到响应会按 §9.22 自我 fencing",
            )
            raise

        # owner 迁移准入代提交(owner-authority.md migrate ③,近似:授权 census 即
        # 准入证据;contract 阶段移交 DS Admission 链)。弱依赖,失败/屏障未开都不
        # 影响心跳。
        if snapshot_present and len(census) > 0:
            await owner_admit_census_weak(
                self.owner_auth,
                self.owner_admitted,
                census,
                OWNER_TYPE_BATTLE,
                ident.pod_name,
                ident.instance_uid,
                OWNER_ADMIT_BUDGET_SEC,
            )

        result = HeartbeatResult(
            accepted_token_gen=out.active.gen,
            accepted_token_jti=out.active.jti,
            accepted_instance_uid=out.active.instance_uid,
            accepted_instance_epoch=out.active.instance_epoch,
            accepted_writer_epoch=out.active.writer_epoch,
        )
        if out.first_activation:
            plog.get().info(
                "battle_ds_credential_activated",
                match_id=match_id,
                pod=ident.pod_name,
                uid=ident.instance_uid,
                epoch=ident.instance_epoch,
                gen=ident.gen,
                jti=ident.jti,
                writer_epoch=ident.writer_epoch,
            )
        if out.battle is not None and out.battle.allocation_id != "":
            try:
                orders = await self.repo.reconcile_player_departures(
                    match_id,
                    BattleDepartureSource(
                        ds_pod_name=ident.pod_name,
                        gameserver_uid=ident.instance_uid,
                        instance_epoch=ident.instance_epoch,
                        allocation_id=out.battle.allocation_id,
                    ),
                    snapshot_present,
                    census_capability_version,
                    census_id,
                    census,
                    acked,
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                plog.get().warning(
                    "battle_heartbeat_refused",
                    reason=REASON_HEARTBEAT_DEPARTURE_RECONCILE,
                    match_id=match_id,
                    pod=ident.pod_name,
                    census_present=snapshot_present,
                    census=len(census),
                    acked_departures=len(acked),
                    err=str(exc),
                    hint=(
                        "Battle→Hub 物理离场对账失败,驱逐单发不出去,玩家可能卡在"
                        "「退出副本」上;下一跳心跳会重试"
                    ),
                )
                raise
            result.eviction_orders = orders
            if len(orders) > 0:
                # R4:心跳是高频路径,成功侧 debug。驱逐单是"玩家退出副本"链上唯一
                # 从后端流向 DS 的指令,查"点了退出没反应"必须能看到它到底发没发出去。
                plog.get().debug(
                    "battle_eviction_orders_issued",
                    match_id=match_id,
                    pod=ident.pod_name,
                    orders=len(orders),
                    census=len(census),
                )
        if out.first_abandon and out.battle is not None:
            # 缺员判弃时只罚缺席者:census 就在手边,直接按它与 roster 求差集,
            # 不必让 data 层把名单再回传一遍(它已经用同一份输入做过判定)。
            absentees: list[int] = []
            if out.roster_incomplete:
                absentees = self.roster_absentees(list(out.battle.player_ids), census)
            finished = await self.finish_empty_abandon(
                match_id,
                out.battle.ds_pod_name,
                out.battle.gameserver_uid,
                out.battle.pod_uid,
                out.battle.allocation_id,
                out.battle.release_track,
                out.battle.instance_epoch,
                list(out.battle.player_ids),
                out.battle.map_id,
                out.battle.game_mode,
                not out.battle.ever_had_players,
                out.roster_incomplete,
                absentees,
            )
            result.command = finished.command
            return result
        if (
            out.terminal
            or out.battle is None
            or out.battle.state == STATE_ENDED
            or out.battle.state == STATE_ABANDONED
        ):
            # R2:一个 if 收敛了四个条件,按判定依据拆成四个 reason —— "DS 被叫停"是
            # 玩家侧「突然被踢回大厅」的直接上游,只知道停了、不知道凭什么停,查不下去。
            stop_reason = REASON_STOP_TERMINAL_AUTH
            if out.battle is None:
                stop_reason = REASON_STOP_BATTLE_MISSING
            elif out.battle.state == STATE_ENDED:
                stop_reason = REASON_STOP_BATTLE_ENDED
            elif out.battle.state == STATE_ABANDONED:
                stop_reason = REASON_STOP_BATTLE_ABANDONED
            plog.get().warning(
                "battle_heartbeat_stop_commanded",
                reason=stop_reason,
                match_id=match_id,
                pod=ident.pod_name,
                uid=ident.instance_uid,
                epoch=ident.instance_epoch,
                # Go 的 `out.Battle.GetState()` 在 nil 时返回零值;Python 侧必须显式
                # 兜住 None,否则这条**诊断日志本身**会抛 AttributeError,
                # 把一个"该停机"变成一个 500。
                state=out.battle.state if out.battle is not None else "",
            )
            result.command = COMMAND_STOP
            return result
        if (
            self.locator is not None
            and out.battle.state in (STATE_READY, STATE_RUNNING)
            and out.battle.ds_addr != ""
            and len(out.battle.player_ids) > 0
        ):
            # fire-and-forget(见模块头「形变 6」),不 await。
            self.refresh_battle_locations(
                list(out.battle.player_ids), match_id, out.battle.ds_addr
            )
        return result

    # ── legacy:非 Model B 心跳 ─────────────────────────────────────────────

    async def heartbeat(
        self,
        match_id: int,
        pod_name: str,
        player_count: int,
        state: str,
        ts_ms: int,
    ) -> HeartbeatResult:
        """处理 DS 上报(单向 unary,DS 每 5s 调)。对应 Go 的 `Heartbeat`。

        刷新 `last_heartbeat_ms` + 状态。镜像不存在(孤儿 DS)→ 返回 stop 指令让其
        自行停机。

        已是终态(ended/abandoned)的镜像:直接返回 stop,且**不写回记录** ——
        不刷新 `last_heartbeat_ms` / TTL,也不重新 ZAdd active。否则 abandoned 后仍在
        心跳的 DS(pod release 失败 / 延迟终止)会不断推迟 sweep 补偿重试并刷新
        BattleTTL 上界,使 active 重新可能无限堆积(W4 ⑧ Codex 复审 P1)。
        """
        return await self.heartbeat_with_census(
            match_id, pod_name, player_count, state, ts_ms, False, None
        )

    async def heartbeat_with_census(
        self,
        match_id: int,
        pod_name: str,
        player_count: int,
        state: str,
        ts_ms: int,
        snapshot_present: bool,
        active_player_ids: list[int] | None,
    ) -> HeartbeatResult:
        """带在场名单的 legacy 心跳(2026-08-04)。对应 Go 的 `HeartbeatWithCensus`。

        相对 `heartbeat` 多做两件事,**与 Model B 心跳
        (`heartbeat_authorized_with_players`)同语义**:①续写 owner 权威实例租约;
        ②对在场玩家代提交 owner Admit。

        为什么必须补:legacy 面此前两件都不做,于是玩家 travel 进战斗 DS、连接也建好了,
        owner 记录却永远停在 PENDING、实例租约永远过期 —— login 的
        `apply_owner_placement` 只在「ADMITTED 且租约剩余 > 安全余量」才报 STABLE,
        客户端因此恒收到 "post-travel owner target is still PENDING",撑到 30s deadline
        弹兜底面板,进不去副本(2026-08-04 mode=local 实测,与 hub 侧同一形状的洞)。

        线上隔离:本方法只在 `redis_authority_enabled()` 为假时被 service 层调用,
        Model B 恒走 `heartbeat_authorized_with_players`,生产路径零变更。
        """
        # ACK 必须在**进入本体之前**快照:终态 / pod_mismatch / 孤儿三条分支都会调
        # kill_stranded_ds,而它是异步任务 —— 本体返回后再读台账,进程记录可能已被
        # Release 删掉,同一条 stop 应答带不带 ACK 就成了竞态。
        # 先快照后回显,应答与调用时刻的授权事实一致。
        ack = self.local_credential_ack(pod_name)
        res = await self.heartbeat_legacy(
            match_id, pod_name, player_count, state, ts_ms, snapshot_present, active_player_ids
        )
        # 回显在**所有** legacy 应答上,含 stop/drain:UE 校验不过时会把 command 连同
        # 驱逐单一起清空(见 `local_allocator.LocalGameServerAllocator.local_credential_ack`
        # 注释),而"让这台 DS 停机"恰恰是最需要送达的一条。
        if ack is not None and res is not None and res.accepted_instance_uid == "":
            res.accepted_token_gen = ack.gen
            res.accepted_token_jti = ack.jti
            res.accepted_instance_uid = ack.instance_uid
            res.accepted_instance_epoch = ack.instance_epoch
            res.accepted_writer_epoch = ack.writer_epoch
        return res

    def local_credential_ack(self, pod_name: str) -> Any | None:
        """取本机战斗 DS 的完整凭据身份(mode=local 才有)。
        对应 Go 的 `localCredentialACK`。

        手法与边界同 hub 侧 `HubUsecase.apply_local_credential_ack`:值取自本进程签发、
        经 env 下发给该 DS 的同一份凭据(不是凭空构造),且严格 fail-closed ——
        pod 不在台账或五元组不全时一律不回显,绝不糊半截 ACK。

        线上隔离(双重机械门):①只有 `redis_authority_enabled()` 为假的 legacy 心跳
        会走到这里;②`LocalBattleCredentialSource` 仅
        `local_allocator.LocalGameServerAllocator` 实现,Agones / Mock 分配器的
        `isinstance` 直接不成立,两条路径的应答逐字节不变。

        ★ 返回类型标注成 `Any | None` 而不是 `BattleCredentialIdentity | None`:
          仓里有**两个**同名 `BattleCredentialIdentity`,这里回的是
          **local_allocator 那份**(多一个 `complete_for_ack()`)。为一行标注把
          进程拉起路径与 DS 拉起模块耦合起来不值得,与 `gameserver.py` 的处理同因。
        """
        if pod_name == "":
            return None
        if not isinstance(self.alloc, LocalBattleCredentialSource):
            return None
        return self.alloc.local_credential_ack(pod_name)

    async def heartbeat_legacy(  # noqa: C901, PLR0912, PLR0913, PLR0915 —— 与 Go 逐条对应
        self,
        match_id: int,
        pod_name: str,
        player_count: int,
        state: str,
        ts_ms: int,  # noqa: ARG002 —— 与 Go 同:形参在,但不参与任何判定
        snapshot_present: bool,
        active_player_ids: list[int] | None,
    ) -> HeartbeatResult:
        """legacy 心跳本体(镜像 CAS + owner 接线 + 位置续期)。
        对应 Go 的 `heartbeatLegacy`。

        凭据 ACK 回显由唯一调用方 `heartbeat_with_census` 统一收口,避免每条 return
        各补一次。
        """
        census: list[int] = list(active_player_ids) if active_player_ids else []

        if match_id == 0:
            plog.get().warning(
                "battle_heartbeat_refused",
                reason=REASON_ALLOC_MATCH_ID_REQUIRED,
                pod=pod_name,
                authority="legacy",
            )
            raise errcode.PandoraError(errcode.ErrInvalidArg, "match_id required")
        now = now_ms()

        # ── CAS 回调的出参(Go 的闭包捕获变量,逐个对应)────────────────────
        #
        # ★ 用一个可变字典而不是 `nonlocal` 一长串标量:回调里要写 18 个出参,
        #   `nonlocal` 漏声明一个不会报错 —— 它会变成回调的**局部变量**,写进去
        #   再也读不出来。那种漏写在"CAS 一次成功"的常规路径上完全不可见,
        #   只有并发重跑时才显形。
        st: dict[str, Any] = {
            # owner 接线所需的 exact 实例身份(CAS 内捕获,与本轮写回的镜像同一快照)。
            "owner_uid": "",
            "owner_track": "",
            "owner_epoch": 0,
            "became_ready": False,
            # 断线重连(docs/design/battle-reconnect.md §2.2):捕获对局在
            # ready/running 时的玩家名单 + ds_addr,心跳成功后续期这些玩家的
            # BATTLE 位置 TTL。回调可能因 CAS 冲突重跑,故每轮重置。
            "refresh_active": False,
            "refresh_addr": "",
            "refresh_players": [],
            # 空场兜底(2026-07-06):对局活跃但 player_count==0 持续超阈 → 判 abandoned
            # (全员掉线未归 / 客户端从未连入,DS 空转烧资源)。主路径是 DS 侧空场计时器
            # 自结算(agones-dev.md §2.4),这里是后端保险。
            #
            # 双阈值(2026-08-07,anti-abuse-scene-entry.md §3.2.1):阈值按
            # ever_had_players 二选一 —— 有人连入过的局要给断线重连留路(默认 5m,
            # 远大于 ~30s 重连窗);从未连入的局根本没人要回来,只需覆盖
            # 「DS 报 ready → travel + 连接 + Admission」(默认 150s)。
            "empty_abandoned": False,
            "abandon_no_show": False,
            "abandon_pod": "",
            "abandon_uid": "",
            "abandon_pod_uid": "",
            "abandon_allocation_id": "",
            "abandon_release_track": "",
            "abandon_instance_epoch": 0,
            "abandon_players": [],
            "abandon_map_id": 0,
            "abandon_game_mode": "",
            # 花名册到齐期限(INC-20260813-001):空场两档都只管「一个人都没有」,
            # 拦不住「来了但没来齐」。缺席者单列,判弃时只罚他们。
            "abandon_roster_incomplete": False,
            "abandon_absentees": [],
            # observe 档的采证出参:到点但本局不允许判弃(mode/代不匹配)时置位,
            # CAS 外打 WARN。
            "roster_would_abandon": False,
            "roster_would_abandon_absentees": [],
            "roster_battle_gen": 0,
        }
        empty_timeout_ms = _duration_ms(self.cfg.empty_battle_timeout_td())
        no_show_timeout_ms = _duration_ms(self.cfg.resolve_no_show_timeout())
        # off 档归零 = 整道闸关死(不记事实、不判弃);与 Model B 路径同一口径(见 helper)。
        roster_deadline_ms = _duration_ms(self.effective_roster_join_deadline())
        roster_arm_window_ms = _duration_ms(self.cfg.resolve_roster_join_arm_window())
        roster_mode = self.resolve_roster_join_mode_safe()

        def _apply(b) -> None:  # noqa: ANN001, C901, PLR0912 —— dspb.BattleStorageRecord
            # CAS 冲突重跑时以最后一轮为准,每轮重置出参标记
            st["refresh_active"] = False
            st["empty_abandoned"] = False
            st["abandon_no_show"] = False
            st["abandon_roster_incomplete"] = False
            st["abandon_absentees"] = []
            st["roster_would_abandon"] = False
            st["roster_would_abandon_absentees"] = []
            st["roster_battle_gen"] = 0
            if b.state in (
                STATE_ALLOCATION_UNCERTAIN,
                STATE_ALLOCATION_RECONCILING,
                STATE_ALLOCATION_EMPTY_FENCE,
                STATE_PREACTIVE_RELEASING,
                STATE_ALLOCATION_ABORT,
            ):
                raise HeartbeatAllocationFencedError
            # 已是终态(ended/abandoned):中止写回(哨兵异常),不刷新 TTL/active,
            # 令 DS 停机
            if b.state in (STATE_ENDED, STATE_ABANDONED):
                raise HeartbeatTerminalError
            # pod_name 校验:镜像已绑定某个 pod,但上报方是另一个 pod
            # (旧 DS / 孤儿 DS / 重分配残留)→ 不写回该镜像,令上报方停机,
            # 避免污染新对局(防进错对局的 DS 刷 state/心跳)。
            if b.ds_pod_name != "" and pod_name != "" and b.ds_pod_name != pod_name:
                raise HeartbeatPodMismatchError
            prev_state = b.state
            b.last_heartbeat_ms = now
            b.player_count = player_count
            if state != "":
                b.state = state
            # warming → ready/running:DS 首次确认就绪,这一跳让 allocate_battle
            # 得以放行 matchmaker。
            if prev_state == STATE_WARMING and b.state in (STATE_READY, STATE_RUNNING):
                st["became_ready"] = True
            # 空场跟踪:活跃对局无人 → 盖 empty_since_ms 起计时;有人回来 → 清零;
            # 持续空场超阈 → 同一 CAS 内直接写 abandoned(与心跳写回原子,无额外竞态窗口)。
            if b.state in (STATE_READY, STATE_RUNNING):
                # 阈值取 ever_had_players ? empty : no-show —— ever_had_players 一经
                # 置位永不清零,因此只要有一个玩家进来过,本局就永久享受长阈值
                # (5v5 里「9 人在打、1 人掉线」绝不会走 no-show)。
                # no_show_timeout_ms<=0 回退长阈值:未配差异化时退化成改动前行为,
                # 绝不能让阈值变 0 导致 no-show 局永不回收(fail-safe:宁可回收晚,
                # 不可不回收)。
                active_timeout_ms = empty_timeout_ms
                if not b.ever_had_players and no_show_timeout_ms > 0:
                    active_timeout_ms = no_show_timeout_ms
                if player_count > 0:
                    b.empty_since_ms = 0
                    b.ever_had_players = True
                elif b.empty_since_ms == 0:
                    b.empty_since_ms = now
                elif active_timeout_ms > 0 and now - b.empty_since_ms >= active_timeout_ms:
                    b.state = STATE_ABANDONED
                    st["empty_abandoned"] = True
                    st["abandon_no_show"] = not b.ever_had_players
                    st["abandon_pod"] = b.ds_pod_name
                    st["abandon_uid"] = b.gameserver_uid
                    st["abandon_pod_uid"] = b.pod_uid
                    st["abandon_allocation_id"] = b.allocation_id
                    st["abandon_release_track"] = b.release_track
                    st["abandon_instance_epoch"] = b.instance_epoch
                    st["abandon_players"] = list(b.player_ids)
                    st["abandon_map_id"] = b.map_id
                    st["abandon_game_mode"] = b.game_mode

                # 花名册到齐期限(INC-20260813-001)。与上面的空场两档并列而不是嵌套:
                # 那两档看 player_count==0,本档看 census 与 roster 的差集 —— 事故当天
                # player_count 恒为 5(非 0),空场计时器一次都没起过,3v2 照常打完。
                #
                # 只在**开局阶段**生效(!roster_ever_complete):曾经到齐过的局此后任何
                # 掉线都交回 empty_battle_timeout,否则局中掉线会在 deadline 后判弃一场
                # 正打着的对局。
                #
                # 武装窗(§5 滚动升级):roster_ever_complete 是**新版**副本才写的记忆,
                # 旧副本手里已经到齐过的局不会有它。滚动升级时那种局一旦被新副本接手,
                # 光看标记会把「局中掉线」误判成「开局没到齐」,45s 后判弃一场**正在打
                # 的对局**。时间窗只保护已经过窗的老局;窗口内旧副本见过到齐、新副本
                # 接手时正好缺人的局仍可能被误武装。它是纵深防御,发布仍须
                # observe → policy generation enforce。
                if (
                    not st["empty_abandoned"]
                    and not b.roster_ever_complete
                    and roster_deadline_ms > 0
                    and snapshot_present
                    and roster_gate_armable(b.allocated_at_ms, now, roster_arm_window_ms)
                ):
                    absent = self.roster_absentees(list(b.player_ids), census)
                    if len(absent) == 0:
                        # 全员同时在场过一次就永久豁免。
                        b.roster_ever_complete = True
                        b.roster_incomplete_since_ms = 0
                    elif b.roster_incomplete_since_ms == 0:
                        b.roster_incomplete_since_ms = now
                    elif now - b.roster_incomplete_since_ms >= roster_deadline_ms:
                        # 到点。能不能**真**判弃由激活档 + 策略代把关(共享谓词,
                        # 与 data 层同一份):observe 只采证;enforce 也只对
                        # 「当前配置代创建」的局动手 —— legacy(gen=0)与旧代局永不执行,
                        # 滚动升级窗口的误判弃在机制上不可达。
                        if not dsconf.roster_deadline_should_abandon(
                            roster_mode,
                            self.cfg.roster_policy_generation,
                            b.roster_policy_generation,
                        ):
                            st["roster_would_abandon"] = True
                            st["roster_would_abandon_absentees"] = absent
                            st["roster_battle_gen"] = b.roster_policy_generation
                        else:
                            b.state = STATE_ABANDONED
                            st["empty_abandoned"] = True
                            st["abandon_roster_incomplete"] = True
                            # 罚只记缺席者,不按 no-show 全员记
                            st["abandon_no_show"] = False
                            st["abandon_absentees"] = absent
                            st["abandon_pod"] = b.ds_pod_name
                            st["abandon_uid"] = b.gameserver_uid
                            st["abandon_pod_uid"] = b.pod_uid
                            st["abandon_allocation_id"] = b.allocation_id
                            st["abandon_release_track"] = b.release_track
                            st["abandon_instance_epoch"] = b.instance_epoch
                            st["abandon_players"] = list(b.player_ids)
                            st["abandon_map_id"] = b.map_id
                            st["abandon_game_mode"] = b.game_mode
            # 对局活跃(ready/running,且未被空场超时判弃):记下玩家名单 + ds_addr,
            # 供心跳后续期 BATTLE 位置。
            if b.state in (STATE_READY, STATE_RUNNING):
                st["refresh_active"] = True
                st["refresh_addr"] = b.ds_addr
                st["refresh_players"] = list(b.player_ids)
                st["owner_uid"] = b.gameserver_uid
                st["owner_epoch"] = b.instance_epoch
                st["owner_track"] = b.release_track

        try:
            await self.repo.update_battle_with_lock(
                match_id, UPDATE_MAX_RETRY, _apply, self.battle_ttl_sec()
            )
        except asyncio.CancelledError:
            raise
        except HeartbeatAllocationFencedError:
            plog.get().warning(
                "heartbeat_allocation_fenced_stop", match_id=match_id, pod=pod_name
            )
            return HeartbeatResult(command=COMMAND_STOP)
        except HeartbeatTerminalError:
            # 终态 DS:不写回、通知停机,补偿重试与 TTL 上界不受影响
            plog.get().info("heartbeat_terminal_stop", match_id=match_id, pod=pod_name)
            self.kill_stranded_ds(match_id, pod_name, "terminal")
            # owner 精确释放(legacy 正常结算的**真实**收口点,2026-08-04;
            # INC-20260804-001 缺口⑦)。
            #
            # 为什么在这里而不是 release_battle:legacy 面对局正常结束(含 PVE 主动
            # 退出)后,battle_result 记账 + outbox 投递完成,DS 转 ended 并继续上报心跳,
            # 由**本分支**判终态并回收 DS —— `release_battle` 在这条流程里**一次都不会
            # 被调用**(实测计数 0)。此前把释放接在 release_battle 上是接错了位置,
            # owner 因此仍停在 BATTLE/ADMITTED 指向一台已销毁的 DS:login 的 query-first
            # 一直把玩家指回去,而对局记录已终态,客户端拿到的 TARGET 缺 match_id →
            # `incomplete owner identity` → 撞 30s 线,玩家打完副本回不了大厅
            # (2026-08-04 两轮实测)。
            #
            # 时序满足 owner_release_abandoned_players_weak 的安全边界①:
            # kill_stranded_ds 已发起本实例回收,此后释放不会在旧 DS 仍可服务时
            # 放行新归属。边界②exact 身份门与③compare-delete 由该函数自身保证 ——
            # 玩家已被分到别处时双重跳过,不误伤。记录读取失败或无实例身份时整体
            # 跳过(退化为改动前行为),绝不用半截身份去删归属。
            try:
                terminal = await self.repo.get_battle(match_id)
            except asyncio.CancelledError:
                raise
            except BaseException as gexc:
                plog.get().warning(
                    "terminal_owner_release_read_failed",
                    match_id=match_id,
                    pod=pod_name,
                    err=str(gexc),
                    hint="owner 未释放,玩家可能回不了大厅;下一跳心跳会重试",
                )
            else:
                if terminal is not None:
                    await owner_release_abandoned_players_weak(
                        self.owner_auth,
                        list(terminal.player_ids),
                        terminal.ds_pod_name,
                        terminal.gameserver_uid,
                        OWNER_RELEASE_BUDGET_SEC,
                    )
            return HeartbeatResult(command=COMMAND_STOP)
        except HeartbeatPodMismatchError:
            # pod 不匹配:不写回镜像,令旧/孤儿 DS 停机(防污染新对局)
            plog.get().warning("heartbeat_pod_mismatch", match_id=match_id, pod=pod_name)
            self.kill_stranded_ds(match_id, pod_name, "pod_mismatch")
            return HeartbeatResult(command=COMMAND_STOP)
        except BaseException as exc:
            if errcode.as_code(exc) == errcode.ErrDSPodNotFound:
                # 孤儿 DS:无镜像,通知停机
                plog.get().warning("heartbeat_orphan_ds", match_id=match_id, pod=pod_name)
                self.kill_stranded_ds(match_id, pod_name, "orphan")
                return HeartbeatResult(command=COMMAND_STOP)
            # 上面三条哨兵之外的失败(乐观锁重试耗尽、Redis 不可用等)以前直接透传,
            # 后端零日志。它会让本跳心跳不落库,连续发生就是 15s 判弃的前因。
            plog.get().warning(
                "battle_heartbeat_refused",
                reason=REASON_HEARTBEAT_UPDATE_FAILED,
                match_id=match_id,
                pod=pod_name,
                state=state,
                player_count=player_count,
                authority="legacy",
                code=errcode.as_code(exc),
                err=str(exc),
            )
            raise

        if st["became_ready"]:
            # R1:warming → ready/running 的首次迁移,是"DS 起来了"的唯一凭证,也是
            # allocate_battle 得以放行 matchmaker 的那一跳。每对局至多一条,升 info。
            plog.get().info(
                "battle_ds_heartbeat_ready",
                match_id=match_id,
                pod=pod_name,
                state=state,
                player_count=player_count,
                authority="legacy",
            )
        if st["roster_would_abandon"]:
            # observe 采证 / 代不匹配豁免:到点了但本局不允许真判弃。这条 WARN 是
            # 激活协议的唯一现场依据 —— enforce 翻闸前,运维靠它判断 45s 会误伤多少
            # 「本可成立」的局(每 5s 心跳一条,持续到人补齐或对局被其它闸收走;
            # 量大本身就是「别翻 enforce」的证据)。
            plog.get().warning(
                "roster_incomplete_would_abandon",
                match_id=match_id,
                pod=pod_name,
                mode=roster_mode,
                battle_policy_generation=st["roster_battle_gen"],
                config_policy_generation=self.cfg.roster_policy_generation,
                absent_players=st["roster_would_abandon_absentees"],
                deadline_ms=roster_deadline_ms,
                authority="legacy",
                hint="mode=enforce 且 battle 代==配置代 才会真判弃;见 decision-revisit §5",
            )
        if st["empty_abandoned"]:
            # 空场 / 缺员超时判弃:回收 pod + 投递补偿 + 移出 active,回 stop 指令令 DS 停机。
            return await self.finish_empty_abandon(
                match_id,
                st["abandon_pod"],
                st["abandon_uid"],
                st["abandon_pod_uid"],
                st["abandon_allocation_id"],
                st["abandon_release_track"],
                st["abandon_instance_epoch"],
                st["abandon_players"],
                st["abandon_map_id"],
                st["abandon_game_mode"],
                st["abandon_no_show"],
                st["abandon_roster_incomplete"],
                st["abandon_absentees"],
            )
        # owner 权威实例租约双写 + 在场玩家准入代提交(与 Model B 心跳同语义,
        # 见方法注释)。时序同 owner-authority.md §4:必须在心跳响应返回前完成租约续写。
        # 身份取不出(mock 镜像无实例身份)则整段跳过,保持旧行为。
        if st["refresh_active"] and st["owner_uid"] != "":
            await renew_owner_lease_gate(
                self.owner_lease,
                self.owner_lease_required,
                pod_name,
                st["owner_uid"],
                st["owner_epoch"],
                st["owner_track"],
            )
            # 弱依赖:失败/屏障未开都不影响心跳(下一跳重试)。
            # 优先用 DS 真实上报的在场名单(exact);拿不到时按下面的 local-off-v1 兜底。
            admit_players = census
            if not snapshot_present or len(admit_players) == 0:
                # local-off-v1 兜底:该档位下 census **结构上不可能成立** —— UE 的
                # BuildCompleteBattlePlayerCensus 要求每个 owner 的 claims 满足
                # IsCompleteBattleOwnerClaims(dst_ver==2 且带
                # ds_pod/ds_uid/ds_epoch/allocation_id),而本档位刻意只签 HS256 legacy
                # 战斗票(GrpcDSAllocator.SignBattleTicket 注释:UE DS 硬锁
                # HS256LocalOff、不交叉接受 v2),legacy 战斗票又**带不了**实例绑定
                # (pkg/auth.signDSTicket 明令 binding 只许 hub 票用)。三者叠加 =
                # census 永远缺席 = owner 永远停在 PENDING = 客户端进图后永远等不到
                # STABLE,30s 后弹"重连时间较长"(2026-08-04 实测:副本内其实能正常
                # 战斗,只是入场确认不了)。
                #
                # 兜底用「本对局花名册」代替在场名单,并要求 player_count>0
                # (DS 确认确有人连入)。这是**近似**,与 Model B 的 census(exact)有别:
                # 名册里可能有还在 travel 的玩家,会被提前判 ADMITTED。可接受的理由:
                # ①owner 侧 Admit 仍做 exact 身份 CAS(pod/uid/epoch/operation_id 全等)
                # + admit_not_before 屏障,归属指向别处的玩家一律被拒,不会凭空造出
                # 第二个 owner;②Begin 早已把归属搬到本实例,旧 DS 此刻已无归属,
                # 提前 ADMITTED 不产生双 DS;③本路径只在 legacy(非 Model B)心跳上,
                # 生产恒走 heartbeat_authorized_with_players 的 exact census,
                # 一字不受影响。一旦 local 档位将来能签带绑定的战斗票,census 自然非空,
                # 兜底自动让位。
                if player_count > 0:
                    admit_players = st["refresh_players"]
            if len(admit_players) > 0:
                await owner_admit_census_weak(
                    self.owner_auth,
                    self.owner_admitted,
                    admit_players,
                    OWNER_TYPE_BATTLE,
                    pod_name,
                    st["owner_uid"],
                    OWNER_ADMIT_BUDGET_SEC,
                )
        # 断线重连(docs/design/battle-reconnect.md §2.2):对局活跃时续期玩家 BATTLE
        # 位置 TTL,使玩家整局在线期间 login 都能检测到"在战斗中",支持中途掉线重登
        # 直连回原 battle DS。
        if (
            self.locator is not None
            and st["refresh_active"]
            and st["refresh_addr"] != ""
            and len(st["refresh_players"]) > 0
        ):
            # fire-and-forget(见模块头「形变 6」),不 await。
            self.refresh_battle_locations(
                list(st["refresh_players"]), match_id, st["refresh_addr"]
            )
        return HeartbeatResult(command=COMMAND_NONE)

    # ── 花名册到齐期限(INC-20260813-001)的档位口径 ─────────────────────────

    def resolve_roster_join_mode_safe(self) -> str:
        """到齐期限激活档;配置非法时滑向 `observe`(只采证,绝不判弃)。
        对应 Go 的 `resolveRosterJoinModeSafe`。

        `main` 启动时已 fail-fast(`conf.resolve_roster_join_mode()` 对未知档位抛),
        此兜底正常不可达 —— 留它是因为本方法在**判弃路径**上,不可达分支的失败方向
        也必须是安全侧:拼错一个字母不该悄悄变成"判弃一场正在打的对局"。

        ★ 只捕 `ValueError`(conf 唯一会抛的类型)而不是宽 except:把 Redis /
          编程错误一起吞成 observe,会让"整道闸悄悄不生效"混进"配置写错了"里。
        """
        try:
            return self.cfg.resolve_roster_join_mode()
        except ValueError:
            return dsconf.ROSTER_JOIN_MODE_OBSERVE

    def effective_roster_join_deadline(self) -> _dt.timedelta:
        """「激活档折算后的」到齐期限:off 档归零。对应 Go 的 `effectiveRosterJoinDeadline`。

        off 归零 = 整道闸关死(连事实都不记)。legacy 与 Model B 两条心跳路径必须用
        同一个口径,否则 off 只关一半 —— 一条路径不判弃、另一条照判,而 DS 落在哪条
        取决于 `redis_authority_enabled()`,现场几乎不可能靠日志分辨。

        ★ 返回 `timedelta` 而非秒:两处调用一处要秒(喂
          `BattleHeartbeatInput.roster_join_deadline_sec`)、一处要毫秒(legacy 的
          `roster_deadline_ms`),用 timedelta 才能两处都不做二次换算。
        """
        if self.resolve_roster_join_mode_safe() == dsconf.ROSTER_JOIN_MODE_OFF:
            return _dt.timedelta(0)
        return self.cfg.resolve_roster_join_deadline()

    # ★ 模块级函数(与 Go 的包级 `rosterAbsentees` 同构)+ 类内 `staticmethod` 绑定。
    #   两个调用点都写成 `self.roster_absentees(...)`,但实现只有一份:再包一层
    #   `def roster_absentees(self, ...): return roster_absentees(...)` 只会多出一处
    #   可以和本体漂移的形参列表。RHS 在类体求值时类命名空间里还没有这个名字,
    #   因此解析到模块全局,不会自指。
    roster_absentees = staticmethod(roster_absentees)

    # ── no-show 记账 → 进入侧退避(anti-abuse §6 第 8 项)────────────────────

    async def record_no_show_penalties(self, match_id: int, player_ids: list[int]) -> None:
        """对判弃 roster 记账并按温和档指数退避布罚。对应 Go 的 `recordNoShowPenalties`。

        记账窗口(默认 10min)内首 `no_show_penalty_free`(默认 1)次免罚,之后
        `base × 2^k` 封顶 `cap`(默认 30s→60s→120s→240s→5min)。惩罚的**执行点**在
        matchmaker `StartMatch`(读 noshowcd 键拒绝 + 可见倒计时),本方法只落账。

        **全程 fail-open**:任何 Redis 错误只 Warn,绝不阻断判弃收尾 —— 记罚是背压,
        判弃回收才是正确性(§9 不变量 4)。一次记账失败让刷子少挨一次罚;一次判弃失败
        让一台 14Gi Pod 永远占着。

        **幂等性来自调用点而非本方法**:唯一调用者 `finish_empty_abandon` 只在
        `first_abandon` / `empty_abandoned` 为真时进入,而那两个标记由 CAS 的
        「状态迁移全局只有一个 EXEC 能成功」保证每局至多置位一次
        (见 `repo._update_with_lock` 的 fn 重跑契约)。本方法自身**不去重**,
        因此绝不可在别处新增调用点 —— 那等于给同一批玩家重复加码退避。
        """
        if self.no_show_recorder is None:
            return
        window = self.cfg.no_show_ledger_window_td()
        base = self.cfg.no_show_penalty_base_td()
        # 判据 `window <= 0 or base <= 0`(注意是 `<=` 不是 `== 0`:负值 = 显式关闭)
        # 由 conf 收口,两处各写一遍必然漂移成"关记罚只关一半"。
        if not self.cfg.no_show_penalty_enabled():
            return
        penalty_cap = self.cfg.no_show_penalty_cap_td()
        # 负值 = 严格档「首次即罚」,在 conf 消费端钳成 0。忘了钳会让 `count - free`
        # 因负 free 变大 —— 严格档反而**加重**处罚,与注释语义相反。
        free = self.cfg.resolve_no_show_penalty_free()
        for pid in player_ids:
            try:
                count = await self.no_show_recorder.record_no_show(
                    pid, window.total_seconds()
                )
            except asyncio.CancelledError:
                # ★ 必须紧邻宽 except 之上:取消是停机控制流,吞掉它会让判弃收尾
                #   在优雅排空期空转一整轮 roster。
                raise
            except BaseException as exc:  # noqa: BLE001 —— fail-open,失败只告警
                plog.get().warning(
                    "noshow_ledger_record_failed",
                    match_id=match_id,
                    player_id=pid,
                    err=str(exc),
                )
                continue
            over = count - free
            if over <= 0:
                continue  # 免罚额度内(偶发一次不惩罚,温和档核心)
            # base << (over-1),指数移位钳到上限防溢出;cap 兜底。
            shift = over - 1
            if shift > NO_SHOW_PENALTY_MAX_SHIFT:
                shift = NO_SHOW_PENALTY_MAX_SHIFT
            penalty = base * (2**shift)
            if penalty_cap > _dt.timedelta(0) and penalty > penalty_cap:
                penalty = penalty_cap
            try:
                await self.no_show_recorder.arm_penalty(pid, penalty.total_seconds())
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001 —— fail-open,失败只告警
                plog.get().warning(
                    "noshow_penalty_arm_failed",
                    match_id=match_id,
                    player_id=pid,
                    err=str(exc),
                )
                continue
            # 拒绝与惩罚走日志定位玩家(§4.4:player_id 绝不能进 metrics label)。
            plog.get().warning(
                "noshow_penalty_armed",
                match_id=match_id,
                player_id=pid,
                window_count=count,
                # Go 是 `penalty.String()`;`godur.duration_string` 是其逐字等价,
                # 不能改成秒数 —— 告警规则与 runbook 都按 "30s"/"2m0s" 这种串匹配。
                penalty=godur.duration_string(penalty),
            )

    # ── 空场 / 缺员判弃的收尾(§9 不变量 4:DS 崩溃必有补偿)──────────────────

    async def finish_empty_abandon(  # noqa: C901, PLR0912, PLR0913 —— 与 Go 逐条对应
        self,
        match_id: int,
        pod_name: str,
        instance_uid: str,
        pod_uid: str,
        allocation_id: str,
        release_track: str,
        instance_epoch: int,
        player_ids: list[int],
        map_id: int,
        game_mode: str,
        no_show: bool,
        roster_incomplete: bool,
        absentees: list[int],
    ) -> HeartbeatResult:
        """完成空场超时判弃的收尾。对应 Go 的 `finishEmptyAbandon`。

        `abandoned` 已在心跳 CAS 内写入镜像;本方法做剩下三件:回收 pod + 投递
        `ds.lifecycle{ABANDONED}` 补偿事件 + 移出 active,并令 DS 停机。

        **投递失败的重试闭环**(§9 不变量 4 可靠补偿):投递失败时对局保留在 active;
        Model B 的 TERMINATING 两键保持**永久**,直到外部 release 与 lifecycle 都明确
        成功后才恢复有界 TTL;legacy 仍以原 BattleTTL 为天然上界。

        ★ 每条失败分支都 `return HeartbeatResult(command=COMMAND_STOP)` 而不是抛:
          DS 无论如何都该停机,而墓碑该不该过期是**另一个**判断。把两者合并成"抛异常
          让上层重试"会让 DS 继续跑着心跳,把 sweep 的补偿重试一轮轮往后推。

        Returns:
            `HeartbeatResult(command=COMMAND_STOP)` —— 所有路径同一出参。
        """
        # reason 区分三档:no_show = 从头到尾没人连入(可能是刷进出副本的滥用,也可能是
        # 客户端进场链路断了);all_disconnected = 有人打过但全员掉线未归;
        # roster_incomplete = 来了但没来齐(INC-20260813-001)。
        # 三者的排查方向完全不同,日志必须能分开统计(否则滥用会被当成"玩家网络差")。
        reason, timeout = "all_disconnected", self.cfg.empty_battle_timeout_td()
        if roster_incomplete:
            reason, timeout = "roster_incomplete", self.cfg.resolve_roster_join_deadline()
        elif no_show:
            reason, timeout = "no_show", self.cfg.resolve_no_show_timeout()
        plog.get().warning(
            "battle_abandoned_empty_timeout",
            match_id=match_id,
            pod=pod_name,
            reason=reason,
            empty_timeout=godur.duration_string(timeout),
            roster=len(player_ids),
            absentees=absentees,
        )
        if roster_incomplete:
            # **只罚缺席者**。在场那几位是受害者:他们按时连进来了,局却因为别人没到而
            # 作废,再给他们记一笔退避等于让「队友掉线」变成自己的惩罚(温和档精神)。
            await self.record_no_show_penalties(match_id, absentees)
        elif no_show:
            # no-show 记账 → 进入侧退避。放在判弃 CAS 已提交之后的收口点:
            # first_abandon / empty_abandoned 保证每局至多进入一次,天然不重复记账。
            # 只罚 no_show —— all_disconnected 是「打过但掉线」,可能是网络问题,不记。
            await self.record_no_show_penalties(match_id, player_ids)

        expected: AuthoritativeGameServerAllocation | None = None
        if self.model_b:
            fence = BattleExpectedInstance(
                allocation_id=allocation_id,
                instance_uid=instance_uid,
                instance_epoch=instance_epoch,
            )
            try:
                resolved_pod_uid = await self.ensure_durable_release_pod_uid(
                    match_id, pod_name, fence, release_track
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001 —— 转 stop,不抛
                plog.get().warning(
                    "empty_abandon_pod_uid_preflight_failed",
                    match_id=match_id,
                    pod=pod_name,
                    err=str(exc),
                )
                return HeartbeatResult(command=COMMAND_STOP)
            pod_uid = resolved_pod_uid
            # Go 是 `terminated, terr := ...`:Python 侧异常即 `terminated=False`,
            # 两个出参因此要分开接,不能写成 `try: ... except: return stop`——
            # 那会把「记录已被推进走」(返回 False,无异常)这条**正常**的零副作用
            # 结果与存储故障混成同一条日志。
            terminated = False
            terr: BaseException | None = None
            try:
                terminated = await self.auth_repo.terminate_expected(
                    match_id,
                    fence,
                    STATE_ABANDONED,
                    self.ds_credential_ttl_sec,
                    self.battle_ttl_sec(),
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001 —— 与 Go 的 (false, err) 同语义
                terr = exc
            if not terminated:
                plog.get().warning(
                    "empty_abandon_terminate_fence_failed",
                    match_id=match_id,
                    pod=pod_name,
                    err=str(terr) if terr is not None else None,
                )
                return HeartbeatResult(command=COMMAND_STOP)
            if terr is not None:
                # 照抄 Go 的第二个 `if terr != nil`。它在**两边都不可达**
                # (`terminate_expected` 只会返回 (True, nil) 或 (False, err/nil)),
                # 保留是为了让"data 层将来把索引清理错误升级成 (true, err)"时,
                # biz 侧不需要同步改动就仍有一条现场日志 —— 而不是静默丢掉。
                plog.get().warning(
                    "empty_abandon_index_cleanup_failed", match_id=match_id, err=str(terr)
                )
            expected = AuthoritativeGameServerAllocation(
                pod_name=pod_name,
                instance_uid=instance_uid,
                allocation_id=allocation_id,
                pod_uid=pod_uid,
                instance_epoch=instance_epoch,
                release_track=release_track,
            )
        try:
            await self.release_game_server(match_id, pod_name, expected)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001 —— legacy 面继续走投递,见下
            plog.get().warning(
                "empty_abandon_release_failed",
                match_id=match_id,
                pod=pod_name,
                err=str(exc),
            )
            if self.model_b:
                # 永久 TERMINATING fence 留在 Redis;不得继续投递 / Expire 后让墓碑消失。
                return HeartbeatResult(command=COMMAND_STOP)
        if await self.deliver_abandoned(
            match_id, pod_name, instance_uid, player_ids, map_id, game_mode
        ):
            if self.model_b:
                target = placement.Target(
                    pod_name=pod_name,
                    instance_uid=instance_uid,
                    instance_epoch=instance_epoch,
                    allocation_id=allocation_id,
                    release_track=release_track,
                )
                try:
                    await self.lifecycle_proof_repo.record_allocation_lifecycle_published(
                        match_id, target
                    )
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001 —— 转 stop,墓碑保持永久
                    plog.get().warning(
                        "empty_abandon_lifecycle_marker_failed",
                        match_id=match_id,
                        allocation_id=allocation_id,
                        err=str(exc),
                    )
                    return HeartbeatResult(command=COMMAND_STOP)
                # Go 在这里**重新声明**了一次 fence(同值)。Python 是函数作用域,
                # 上面那个仍然可见,但照样新建一份:靠"上面刚好也叫 fence 且值相同"
                # 成立的正确性,会在有人给上半段加一行赋值时无声消失。
                expire_fence = BattleExpectedInstance(
                    allocation_id=allocation_id,
                    instance_uid=instance_uid,
                    instance_epoch=instance_epoch,
                )
                expired = False
                eerr: BaseException | None = None
                try:
                    expired = await self.auth_repo.expire_terminated_expected(
                        match_id,
                        expire_fence,
                        self.ds_credential_ttl_sec,
                        self.battle_ttl_sec(),
                    )
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001 —— 与 Go 的 (false, err) 同语义
                    eerr = exc
                if eerr is not None or not expired:
                    plog.get().warning(
                        "empty_abandon_expire_failed",
                        match_id=match_id,
                        expired=expired,
                        err=str(eerr) if eerr is not None else None,
                    )
            else:
                try:
                    await self.repo.expire_battle(match_id, self.battle_ttl_sec())
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001 —— legacy 有 BattleTTL 兜底
                    plog.get().warning(
                        "empty_abandon_expire_failed", match_id=match_id, err=str(exc)
                    )
        return HeartbeatResult(command=COMMAND_STOP)

    # ── BATTLE 位置续期(断线重连,弱依赖)──────────────────────────────────

    def refresh_battle_locations(
        self, player_ids: list[int], match_id: int, ds_addr: str
    ) -> None:
        """异步续期一批玩家的 BATTLE 位置 TTL。对应 Go 的 `refreshBattleLocations`。

        **fire-and-forget**,四条缺一不可(与 `kill_stranded_ds` 同一形状):

          - **入口拷贝** `players = list(player_ids)`:调用方传的是
            `list(b.player_ids)` 或 `st["refresh_players"]`,而 CAS 回调可能重跑并
            改写它们;不拷贝就等于把一个**活的**视图交给后台协程。
          - 走 `safego.spawn` 并带名字(裸 `asyncio.create_task` 的异常会被静默吞进
            Task,直到 GC 才打一条认不出主人的 "never retrieved")。
          - **独立短超时** `LOCATION_REFRESH_TIMEOUT_SEC`:locator 卡死既不给心跳响应
            加尾延迟,也不泄漏协程。
          - best-effort:失败只 Warn,绝不影响心跳 / 对局。

        ★ Go 侧用 `plog.Detach(ctx)` 显式剥掉请求 ctx(§16.7:请求 ctx 含 Kratos
          transport / metadata,不得逃逸进后台协程,下游 locator 是挂 Trace middleware
          的 gRPC client)。Python 侧本方法**不接受 ctx 参数** —— 结构上就不可能把请求
          上下文带进去,是同一条纪律的更强形式。
        ★ `self.locator` 在**进入协程前**取值:装配期之后它只读,但把解引用留到协程里
          意味着"None 时炸在一条被 warn 吞掉的后台任务里",现场只剩一行 AttributeError。
          调用方(两条心跳路径)已各自做过 `self.locator is not None`,这里取到的就是
          它们判过的那一个。
        """
        players = list(player_ids)  # 拷贝,脱离调用方列表复用
        locator = self.locator

        async def _run() -> None:
            try:
                async with asyncio.timeout(LOCATION_REFRESH_TIMEOUT_SEC):
                    await locator.refresh_battle_locations(players, match_id, ds_addr)
            except asyncio.CancelledError:
                # ★ 必须紧邻宽 except 之上:CancelledError 继承 BaseException,
                #   被吞掉会让停机时这条任务不退出。
                raise
            except BaseException as exc:  # noqa: BLE001 —— 弱依赖,失败只告警
                plog.get().warning(
                    "refresh_battle_locations_failed", match_id=match_id, err=str(exc)
                )

        safego.spawn(LOCATION_REFRESH_TASK_NAME, _run)

    # ── RPC 4:ListBattles ─────────────────────────────────────────────────

    async def list_battles(self, state_filter: str) -> list[Any]:
        """列出当前战斗实例,`state_filter` 非空时按 state 过滤。对应 Go 的 `ListBattles`。

        Returns:
            `list[dspb.BattleInfo]`。

        Raises:
            权威索引读失败**原样上抛**(返回半截列表会让运维把"Redis 挂了"看成
            "对局都结束了")。

        **读取侧上限(§9 不变量 18)**:本 RPC 不接受客户端参数、也不面向客户端 ——
        它是 gm / 运维口,返回集被 **active ZSET 的规模**硬性兜住,而该集合本身由
        `remove_active` / `expire_battle` / sweep 自愈持续收敛,不是"客户端可写入的
        累积列表"。因此**不**在这里另加一个 Go 侧没有的 LIMIT:凭空加一个截断会让
        "同时有多少局在跑"这个唯一的对账口径静默失真,而漏掉的恰恰是超载时最该看到的
        那几百局。真正的上界纪律在写入侧(每局一项)与 `battle_list_partial` 的
        `active` / `returned` 两个计数上 —— 它们一旦背离就是索引残留的信号。

        ★ 逐条读失败 / 索引残留在循环里被静默 `continue` 掉。逐条打会刷屏(active
          集合可达千级),故按 R4 聚合:只记数量 + 首错 + 一个样本,循环后一条。
        """
        try:
            match_ids = await self.repo.range_active_battles()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            plog.get().warning(
                "battle_list_refused",
                reason=REASON_HEARTBEAT_AUTHORITY_READ,
                state_filter=state_filter,
                err=str(exc),
            )
            raise
        out: list[Any] = []
        read_failed = 0
        index_orphan = 0
        first_err: BaseException | None = None
        sample_match_id = 0
        for mid in match_ids:
            # Go 是 `b, found, gerr := u.repo.GetBattle(...)`,两种失败分开计数:
            # read_failed = 存储真的读不动(要查 Redis);
            # index_orphan = active ZSET 有项但权威镜像已不在(TTL 到期 / 已删,由
            # sweep 自愈,不用查)。混成一个数会让一次 Redis 故障看起来像"索引脏了"。
            try:
                b = await self.repo.get_battle(mid)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001 —— 聚合后循环外一条
                read_failed += 1
                if first_err is None:
                    first_err, sample_match_id = exc, mid
                continue
            if b is None:
                index_orphan += 1
                if sample_match_id == 0:
                    sample_match_id = mid
                continue
            if state_filter != "" and b.state != state_filter:
                continue
            # 只搬客户端 / 运维可见的六个字段(§9 不变量 14):**不**把
            # BattleStorageRecord 整条转出去 —— 它带 pod_uid / allocation_id /
            # release_track 这类内部实例身份。
            out.append(
                dspb.BattleInfo(
                    match_id=b.match_id,
                    ds_pod_name=b.ds_pod_name,
                    ds_addr=b.ds_addr,
                    state=b.state,
                    player_count=b.player_count,
                    allocated_at_ms=b.allocated_at_ms,
                )
            )
        if read_failed > 0 or index_orphan > 0:
            plog.get().warning(
                "battle_list_partial",
                active=len(match_ids),
                returned=len(out),
                read_failed=read_failed,
                index_orphan=index_orphan,
                state_filter=state_filter,
                sample_match_id=sample_match_id,
                first_err=str(first_err) if first_err is not None else None,
                hint="index_orphan = active ZSET 有项但权威镜像已不在(TTL 到期/已删),由 sweep 自愈",
            )
        return out


# ★ `dspb` 现已被 `list_battles` 的 `dspb.BattleInfo` 真正消费;下面这行保留是因为
#   `HeartbeatResult.eviction_orders` 的元素类型与
#   `legacy_pod_uid_preflight_credential_matches` 的 `cred` 形参只在注释里提到
#   `dspb.BattleEvictionOrder` —— 显式引用一次,免得那些注释成为没有出处的字符串。
_EVICTION_ORDER_TYPE = dspb.BattleEvictionOrder
