"""hub_allocator 的 owner 归属接线 —— 对应 Go 侧
`services/battle/hub_allocator/internal/biz/owner_authority.go`。

三个入口,强弱两档,不要混:

    owner_begin_player / owner_begin_player_guarded
        **强依赖**。hub 归属定案的唯一出口(签票点)。写不进 owner 权威就
        **拒绝本次交付**(§9.22 fail-closed)。放行的后果是玩家可能同时被两台 DS
        认领 —— 宁可拒一次操作,也不要写出一份不自洽的归属。
        拒绝不会把玩家卡死(§9.19/§9.20):调用方上抛,客户端按 §9.23 退避重查,
        owner 恢复即自动继续。

    owner_admit_census_weak
        **弱依赖**。心跳 census 的幂等兜底 + 漂移自愈。它不是第二个权威 ——
        真正的 Admit 提交点在 `AcknowledgeAdmission`(§9.23 服务端完成点)。
        census 之所以还留着,是因为"归属镜像指向本实例、owner 记录没跟上"这种漂移
        **没有别的重试点**;删了就再没人补。所以它必须弱:一个玩家自愈失败
        不能打挂整台 DS 的心跳。

    sweep_stale_owner_admitted
        census 准入缓存的有界闸(§9.18 客户端触发型内存容器必须有界)。
        key 里带 instance_uid,实例销毁后其 key 再也不会被续期 → 必须能老化清掉,
        否则缓存随历史实例 UID 无界增长。

## 刻意"少做"的两件事(migrate → contract 的差别)

  - **不本地判定"同目标就跳过"**。那是先查再存(§9.22 明令禁止):判定与写入
    不在同一线性化点。同 exact 实例的收敛由 owner 在行锁事务内做。
  - **不自铸 operation_id**,传空串让权威铸。调用方每次现铸一个新 UUID 恰恰
    破坏 §9.23 的"一次真实进场用一个稳定 operation_id",幂等键形同虚设。

## 与 Go 的已知差异(都已确认安全)

  - `sync.Map` → 普通 dict。asyncio 单线程,且本模块所有读改写之间不 await,
    不存在 Go 那种真并发写。
  - `time.Time` → `time.monotonic()` 浮点秒。Go 的 `time.Now()` 自带单调读数,
    `Before` / `Sub` 用的就是单调部分,所以 monotonic 才是**等价**的那个,
    而不是 `time.time()`(后者会被 NTP 回拨,让缓存项一次性全被判成过期)。
  - Go 的 `(值, error)` 双返回 → 异常 + `errcode.PandoraError` 的声明槽位
    (`retry_after_ms` / `current_record`),见 owner_lease_client 模块头。
  - `context.WithTimeout(ctx, budget)` → `asyncio.timeout` / 单调 deadline。
"""

from __future__ import annotations

import asyncio
import time
from typing import Awaitable, Callable, Protocol

from pandora.owner.v1 import owner_pb2 as ownerpb

from pandorapy import errcode
from pandorapy import log as plog
from pandorapy import placement
from pandorapy.services.hub_allocator.owner_lease_client import (
    OwnerRecordView,
    OwnerTargetView,
)

# owner 类型 / 阶段常量。★ 从生成代码取值,不手抄字面量 ——
# 手抄的那一刻这份常量就和 proto 脱钩了,改 proto 时这里静默不动。
OWNER_TYPE_HUB = int(ownerpb.OWNER_TYPE_HUB)
OWNER_TYPE_BATTLE = int(ownerpb.OWNER_TYPE_BATTLE)
OWNER_PHASE_PENDING = int(ownerpb.OWNER_PHASE_PENDING)
OWNER_PHASE_ADMITTED = int(ownerpb.OWNER_PHASE_ADMITTED)

# census 已准入缓存项(key = instance_uid|player_id)的最大保鲜期(秒)。
# 活实例每轮心跳 census 对在场玩家续 last-touch;超过本值没续期 = 其所属实例已销毁
# (UID 不再心跳)。取值远大于心跳/census 周期(~5s),活实例项绝不会被误清。
OWNER_ADMITTED_STALE_TTL_SEC = 5 * 60.0

# `dict.get` 的缺省哨兵。用它而不是 `is None`:值域是浮点时间戳,
# 万一有人存进 None,`is None` 会把"存在但值坏了"误判成"不存在",
# 于是那条 fail-safe 续期分支永远走不到。
_MISSING = object()

# guard:在 Query 之后、Begin 之前跑的"当前意图仍然成立"复核。失败即抛。
OwnerBeginGuard = Callable[[], Awaitable[None]]
# resolveTarget:查归属镜像当前指向哪里,返回 (target, 是否查到)。
ResolveTarget = Callable[[int], Awaitable[tuple[OwnerTargetView, bool]]]


class OwnerAuthority(Protocol):
    """owner 权威的调用面(Query/Begin/Admit)。对应 Go 的 biz.OwnerAuthority。

    由 `owner_lease_client.GrpcOwnerLeaseRenewer` 实现(与租约续写共用一条连接)。
    可为 None(未配 owner_addr = owner 服务未部署)。
    """

    async def query_owner(self, player_id: int) -> OwnerRecordView: ...

    async def begin_transition(
        self,
        player_id: int,
        expect_epoch: int,
        operation_id: str,
        owner_type: int,
        target: OwnerTargetView,
    ) -> OwnerRecordView: ...

    async def admit(
        self, player_id: int, owner_epoch: int, operation_id: str, target: OwnerTargetView
    ) -> int: ...


def sweep_stale_owner_admitted(admitted: dict[str, float], cutoff: float) -> None:
    """删除 last-touch 早于 cutoff 的 census 准入缓存项。对应 Go 的 sweepStaleOwnerAdmitted。

    ★ 值不是 float 的也删(理论不可达,所有写入点都写 float)—— 有界性不能建立在
      "理论上不会发生"之上:一个类型坏掉的值会让它**永远**通不过时间比较,
      于是那一项就成了永久驻留的泄漏点。fail-safe 删掉即可,玩家还在场的话
      下一轮 census 会 Query→Admit 补回(至多一次多余往返)。

    best-effort:调用方持有 dict,本函数原地改。
    """
    stale = [
        key
        for key, value in admitted.items()
        # isinstance(True, float) 为 False,所以 bool 也会被当成坏值删掉,符合预期。
        if not isinstance(value, float) or value < cutoff
    ]
    for key in stale:
        admitted.pop(key, None)


def owner_record_exactly_targets(
    rec: OwnerRecordView, owner_type: int, target: OwnerTargetView
) -> bool:
    """回传记录是否**逐格**等于本次 target。对应 Go 的 ownerRecordExactlyTargets。

    这是滚动升级的硬门:旧 owner binary 可能仍把"同物理实例、不同 assignment"
    当成 no-op —— RPC 返回成功,却把**旧** target 原样带回。只看错误码的话,
    调用方会拿着一份指向旧 assignment 的记录去签票。
    """
    return (
        rec.owner_epoch > 0
        # ★ 用 placement.go_trim_space 而不是 str.strip():两者的空白字符表**不同**
        #   (Go 的 unicode.IsSpace 与 Python 的 str.isspace 收录不一致),
        #   同一个 operation_id 在两栈里会得出不同的"是否为空"结论。
        and placement.go_trim_space(rec.operation_id) != ""
        and rec.owner_type == owner_type
        and rec.phase in (OWNER_PHASE_PENDING, OWNER_PHASE_ADMITTED)
        and rec.pod_name == target.pod_name
        and rec.instance_uid == target.instance_uid
        and rec.instance_epoch == target.instance_epoch
        and rec.assignment_or_allocation_id == target.assignment_or_allocation_id
        and rec.release_track == target.release_track
    )


async def begin_one_player(
    auth: OwnerAuthority,
    player_id: int,
    owner_type: int,
    target: OwnerTargetView,
    guard: OwnerBeginGuard | None = None,
) -> OwnerRecordView:
    """把单个玩家的 owner 权威推进到 target。对应 Go 的 beginOnePlayer。

    顺序是 Query(拿 CAS 期望值) → guard(复核当前意图) → Begin。

    ★ Query 失败**必须**上抛,不能当成"没有归属"继续:那正是 §9.22 禁止的
      "冒充 OFFLINE",直接后果是第二台 DS 被放进来。

    ★ 冲突**不按原 target 盲重试**:assignment UUID 没有单调次序,
      EPOCH_CONFLICT 可能恰恰说明"更新的 assignment 已经胜出";
      重查 epoch 后继续写旧 target 会把 winner 回滚。
    """
    rec = await auth.query_owner(player_id)
    if guard is not None:
        await guard()
    next_rec = await auth.begin_transition(player_id, rec.owner_epoch, "", owner_type, target)
    if not owner_record_exactly_targets(next_rec, owner_type, target):
        err = errcode.PandoraError(
            errcode.ErrInvalidState,
            "owner Begin returned a non-exact target player=%d",
            player_id,
        )
        # 把权威回传的那份记录带上,便于排查是"旧 binary no-op"还是别的漂移。
        err.current_record = next_rec
        raise err
    return next_rec


async def owner_begin_player(
    auth: OwnerAuthority | None,
    player_id: int,
    owner_type: int,
    target: OwnerTargetView,
    budget_sec: float,
) -> None:
    """单玩家强 Begin。对应 Go 的 ownerBeginPlayer。

    **刻意是单玩家而不是批量**:hub 的签票点天然一次一个玩家。做成批量就会引入
    "前几个已写进权威、最后一个失败"的部分写入态,而 hub 侧没有 Release 通道
    可以精确回滚。保持单玩家,这类残留在结构上就不可能出现。
    """
    await owner_begin_player_guarded(auth, player_id, owner_type, target, budget_sec, None)


async def owner_begin_player_guarded(
    auth: OwnerAuthority | None,
    player_id: int,
    owner_type: int,
    target: OwnerTargetView,
    budget_sec: float,
    guard: OwnerBeginGuard | None = None,
) -> None:
    """带 guard 的单玩家强 Begin。对应 Go 的 ownerBeginPlayerGuarded。

    auth 为 None = owner_addr 未配(服务未部署),属部署形态问题,不在本函数收敛;
    但 guard 仍要跑 —— 它是调用方自己的前置条件,和 owner 是否部署无关。

    只有携 guard 的路径允许**一次** conflict 重试,严格性来自这个顺序:

        Query(E) → guard(assignment=A) → Begin(expect=E, A)

    最终 assignment=B 发布后,任何新的 A guard 都会失败。guard=None 的通用/census
    路径没有"当前意图"的证明,必须单次 fail-closed。
    """
    if auth is None:
        if guard is not None:
            await guard()
        return

    max_attempts = 2 if guard is not None else 1
    err: BaseException | None = None
    try:
        # 对应 Go 的 context.WithTimeout(ctx, budget):预算耗尽 = 失败 = fail-closed。
        async with asyncio.timeout(budget_sec):
            for attempt in range(max_attempts):
                try:
                    await begin_one_player(auth, player_id, owner_type, target, guard)
                    return
                except asyncio.CancelledError:
                    # ★ 先于宽 except 放行:否则停机时的取消会被吞成"Begin 失败",
                    #   既污染告警,又让本协程不退出、优雅排空失效。
                    raise
                except BaseException as exc:  # noqa: BLE001 —— 失败一律 fail-closed 上抛
                    err = exc
                    if (
                        errcode.as_code(exc) != errcode.ErrOwnerEpochConflict
                        or attempt + 1 == max_attempts
                    ):
                        break
    except TimeoutError as exc:
        # asyncio.timeout 到点后会在 __aexit__ 把内部的 CancelledError 转成 TimeoutError。
        # 接住它是为了让下面那条告警照样打出来(Go 侧 budgetCtx 超时同样会走到告警)。
        err = exc

    if err is None:
        return
    plog.get().warning(
        "owner_begin_failed",
        player_id=player_id,
        err=str(err),
        pod=target.pod_name,
        instance_uid=target.instance_uid,
        hint="contract 强依赖:归属写不进权威即拒绝本次交付,调用方重试",
    )
    raise err


def _census_heal_guard(
    resolve_target: ResolveTarget, player_id: int, tgt: OwnerTargetView
) -> OwnerBeginGuard:
    """造一个"归属镜像仍精确指向 tgt"的 guard。

    ★ 写成工厂而不是循环体里的闭包:循环变量在 Python 里是**后期绑定**的,
      闭包若逃逸出本轮迭代就会读到下一个玩家的 player_id。这里虽然当轮就用完,
      但把绑定固化掉比依赖"当轮就用完"这条会被后人改坏的前提要稳。
    """

    async def guard() -> None:
        current, still_current = await resolve_target(player_id)
        if not still_current or current != tgt:
            raise errcode.PandoraError(
                errcode.ErrInvalidState,
                "owner census target changed before Begin player=%d",
                player_id,
            )

    return guard


async def owner_admit_census_weak(
    auth: OwnerAuthority | None,
    admitted: dict[str, float],
    players: list[int],
    owner_type: int,
    self_pod: str,
    self_uid: str,
    budget_sec: float,
    resolve_target: ResolveTarget | None = None,
) -> None:
    """心跳 census 的幂等 Admit 兜底 + 漂移自愈。对应 Go 的 ownerAdmitCensusWeak。

    弱依赖:任何一个玩家失败都只累计计数、批末汇总一条 Warn,绝不打挂心跳。
    """
    if auth is None:
        return

    # ★ 剪枝必须在"本轮无玩家就早退"**之前**:最后一名玩家离场时 census 为空,
    #   若此时早退,他的 admitted 项就永久残留;等他回流本实例(owner epoch 已推进、
    #   新 PENDING)时会被下面的缓存命中误吞,跳过 Query→Admit —— 于是新纪元的
    #   Admit 永远不会提交。
    prefix = self_uid + "|"
    present = {f"{prefix}{player_id}" for player_id in players}
    for key in [k for k in admitted if k.startswith(prefix) and k not in present]:
        admitted.pop(key, None)
    if not players:
        return  # 剪枝已完成;本轮无玩家可代提交 Admit。

    deadline = time.monotonic() + budget_sec
    query_failed = 0
    admit_failed = 0
    heal_failed = 0
    first_err: BaseException | None = None
    sample_player = 0

    def note_fail(player_id: int, exc: BaseException) -> None:
        nonlocal first_err, sample_player
        if first_err is None:
            first_err, sample_player = exc, player_id

    # now 只取一次(与 Go 的 `now := time.Now()` 同):同一轮 census 写进缓存的
    # last-touch 用同一个基准,老化判定才不会因为遍历耗时而分裂。
    now = time.monotonic()
    try:
        for player_id in players:
            key = f"{prefix}{player_id}"
            value = admitted.get(key, _MISSING)
            if value is not _MISSING:
                # 命中即续期,但只在"接近过期"时才写(降写频)。
                if not isinstance(value, float) or now - value > OWNER_ADMITTED_STALE_TTL_SEC / 2:
                    admitted[key] = now
                continue
            if time.monotonic() >= deadline:
                # 预算耗尽:剩余玩家下次心跳继续(census 每 ~5s 一轮,自然收敛)。
                return

            try:
                rec = await auth.query_owner(player_id)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001 —— 弱依赖:计数后继续下一个玩家
                query_failed += 1
                note_fail(player_id, exc)
                continue

            if (
                rec.owner_type != owner_type
                or rec.pod_name != self_pod
                or rec.instance_uid != self_uid
            ):
                # 记录不指向本实例。两种可能:真实迁移(不干预)或签票点 Begin 失败
                # 留下的漂移(补一次自愈)。用归属镜像 resolve_target 区分:
                # 镜像仍指向本实例 = 漂移。
                if resolve_target is not None:
                    tgt, ok = await resolve_target(player_id)
                    if ok and tgt.pod_name == self_pod and tgt.instance_uid == self_uid:
                        try:
                            await begin_one_player(
                                auth,
                                player_id,
                                owner_type,
                                tgt,
                                _census_heal_guard(resolve_target, player_id, tgt),
                            )
                        except asyncio.CancelledError:
                            raise
                        except BaseException as exc:  # noqa: BLE001 —— 自愈补不上,下轮再补
                            heal_failed += 1
                            note_fail(player_id, exc)
                continue

            if rec.phase == OWNER_PHASE_ADMITTED:
                admitted[key] = now
                continue
            if rec.phase != OWNER_PHASE_PENDING:
                continue

            # target 取**记录自身**的字段:pod/uid 是本调用方独立断言过的部分,
            # 其余交给 owner 侧做 exact 全等校验。
            target = OwnerTargetView(
                pod_name=rec.pod_name,
                instance_uid=rec.instance_uid,
                instance_epoch=rec.instance_epoch,
                assignment_or_allocation_id=rec.assignment_or_allocation_id,
                release_track=rec.release_track,
            )
            try:
                await auth.admit(player_id, rec.owner_epoch, rec.operation_id, target)
                admitted[key] = now
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001 —— 弱依赖:分流成 WAIT 或失败计数
                retry_after = (
                    exc.retry_after_ms if isinstance(exc, errcode.PandoraError) else 0
                )
                if retry_after > 0:
                    # 屏障未开:预期中的 WAIT(旧 DS 租约还没过安全截止),
                    # 下次心跳重试。★ 不能计进失败告警,否则每次正常迁移都刷一片假告警。
                    continue
                admit_failed += 1
                note_fail(player_id, exc)
    finally:
        # 对应 Go 的 defer:预算耗尽提前 return 时同样要把这轮的失败汇总打出来。
        if query_failed + admit_failed + heal_failed > 0:
            plog.get().warning(
                "owner_admit_census_weak_failed",
                players=len(players),
                query_failed=query_failed,
                admit_failed=admit_failed,
                heal_begin_failed=heal_failed,
                sample_player_id=sample_player,
                first_err=str(first_err),
                hint="migrate 弱依赖,再入屏障双门兜底",
            )
