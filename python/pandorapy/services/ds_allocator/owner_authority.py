"""Battle DS 的 owner 归属接线 —— 对应 Go 侧
`services/battle/ds_allocator/internal/biz/owner_authority.go`(owner-authority.md,
contract 阶段)。

这是 `CLAUDE.md §9` 不变量 22「同一玩家同一时刻最多只能在一个可玩 DS」在 battle 侧
的落点。四个入口,**强弱两档**,不要混:

    owner_begin_players            **强依赖**。READY 交付前逐玩家把"这批玩家将由该
                                   Battle 实例 own"写进权威(E+1 / PENDING / 屏障)。
                                   **写不进即拒绝本次交付**(§9.22 fail-closed)。
    owner_verify_players_exact     claim loser / 幂等重试交付 READY 前的**只读门**。
                                   零 Begin / 零 Release 副作用。
    owner_release_abandoned_players_weak
                                   判弃对局后释放仍指向本实例的归属(INC-20260729-002
                                   P0-B1)。弱依赖。
    owner_admit_census_weak        授权心跳 census 首见玩家代提交 Admit。弱依赖 ——
                                   census 是周期性重试点,不该让一个玩家的自愈失败
                                   打挂整台 DS 的心跳。

## 为什么 Begin 一定要 fail-closed(而不是"告警放行")

owner 是归属的唯一权威。写不进去就**无法证明**"这台 DS 有权控制该玩家";此时把
READY 交付出去 = 玩家可能同时被两台 DS 认领,直接踩验收底线第 3 条。
拒绝不会把玩家卡死(§9.19/§9.20):分配失败后撮合按既有补偿链回收 claim,客户端按
§9.23 退避重查,owner 恢复即自动重新分配。

## 为什么部分失败必须**精确回滚**

`BeginTransition` 是纯 CAS、不校验 admit 屏障,下次分配确实能覆盖 —— **前提是还有
下次**。玩家若在本次失败后离线,记录会永久停在 PENDING 指向一台马上被清理掉的 Pod;
而 login 的 query-first 在屏障已开时会把它翻译成 `TARGET + PENDING`,把这台死 Pod 当
exact target 下发给客户端。客户端按 §9.23 重查拿到同一个死目标,重登也一样 ——
玩家有"返回登录"出口却回不去游戏。owner 侧**没有任何归属记录的 TTL / 回收**
(唯一的 sweep 清的是审计流水),所以这个残留不会自己消失。

回滚只撤销本次刚写进去的 `(player, epoch, operation)` 三元组,符合验收底线第 4 条
「补偿只允许精确撤销本次未交付的写」。期间记录若已被别的写者推进,epoch/operation
不再匹配,Release 按迟到调用幂等 no-op,不误伤。

## outcome unknown:唯一不许回滚的失败

Begin 的非 EPOCH_CONFLICT 失败**不能直接当作未提交** —— 服务端可能已经 commit,
只是回包丢了。此时用独立预算回读一次;若回读也不可达,抛
`ErrUnavailable(cause=OwnerBeginOutcomeUnknown)`,要求上层**保留 allocation/Pod 与
本批已写 owner**,绝不能猜测性 cleanup。回滚半批再留下一个未知提交,只会主动制造
更难恢复的不一致。

## 与 Go 的已知差异(都已确认安全)

  - `sync.Map` → 普通 dict。asyncio 单线程,且本模块所有读改写之间不 await。
  - `time.Time` → `time.monotonic()` 浮点秒。Go 的 `time.Now()` 自带单调读数,
    `Before`/`Sub` 用的就是单调部分 —— monotonic 才是**等价**的那个,而不是
    `time.time()`(后者会被 NTP 回拨,让缓存项一次性全被判成过期)。
  - `context.WithTimeout` → 单调 deadline + `asyncio.timeout`。
  - `plog.Detach(ctx)`(= `WithoutCancel` + 保留 trace 值)在 asyncio 里**没有等价
    物**:回读 / 回滚仍然会被调用方任务的取消穿透。这是本移植与 Go 的唯一实质
    行为差异,已在各函数处标注 —— 不写成"吞掉 CancelledError",因为那会让停机时
    整条链退不出去(§7 硬性要求)。
  - `target.source_revision` **原样透传**给 `begin_transition`,本模块不做任何
    `> 0` 之类的旁路判定:来源版本的全序判定在 owner 侧(`source_revision.classify`),
    在这里加一道"看着差不多"的闸只会两边不一致。
"""

from __future__ import annotations

import asyncio
import dataclasses
import time
from typing import Protocol

from pandora.owner.v1 import owner_pb2 as ownerpb

from pandorapy import errcode
from pandorapy import log as plog
from pandorapy import placement
from pandorapy.services.ds_allocator.clients import OwnerRecordView, OwnerTargetView

# owner 类型 / 阶段常量。★ 从生成代码取值,不手抄字面量 —— 手抄的那一刻这份常量
# 就和 proto 脱钩了,改 proto 时这里静默不动(Go 侧写的是 `int8 = 2` 字面量,
# 那是因为它刻意让 biz 不依赖生成代码;Python 侧没有这个约束,就该取真源)。
OWNER_TYPE_HUB = int(ownerpb.OWNER_TYPE_HUB)
OWNER_TYPE_BATTLE = int(ownerpb.OWNER_TYPE_BATTLE)
OWNER_PHASE_PENDING = int(ownerpb.OWNER_PHASE_PENDING)
OWNER_PHASE_ADMITTED = int(ownerpb.OWNER_PHASE_ADMITTED)

# Begin 非 epoch-conflict 失败后的**独立**判定预算(秒)。Go: `ownerBeginReadbackBudget`。
# Begin 的入站预算可能正因超时而失效,故回读必须另起预算;2s 与 QueryOwner 单次 RPC
# 的默认上界同量级,足以确认 requested operation 是否已提交。
OWNER_BEGIN_READBACK_BUDGET_SEC = 2.0

# 部分失败后回滚已写入归属的独立预算(秒)。Go: `ownerBeginRollbackBudget`。
# 取值依据:回滚至多 len(players)-1 次(一局最多 10 人 → ≤9 次)单趟 Release RPC,
# 与 Begin 侧同量级往返;3s 留足余量,又不至于让分配失败路径长时间挂着。待实测复核。
OWNER_BEGIN_ROLLBACK_BUDGET_SEC = 3.0

# census 已准入缓存项(key = `instance_uid|player_id`)的最大保鲜期(秒)。
# Go: `ownerAdmittedStaleTTL`。
#
# 活实例每次心跳 census 对在场玩家续 last-touch;超过本值未续期 = 其所属 Battle 实例
# 已销毁(UID 不再心跳)。Battle DS **打完即销毁、InstanceUID 永不复用**,admitted 项
# 若不老化回收会随累计对局数单调增长,长压测下 OOM(§9.18 进程内容器有界)。
# 取值远大于心跳 / census 周期(~5s),活实例项绝不会被误清。
OWNER_ADMITTED_STALE_TTL_SEC = 5 * 60.0

# `dict.get` 的缺省哨兵。用它而不是 `is None`:值域是浮点时间戳,万一有人存进 None,
# `is None` 会把"存在但值坏了"误判成"不存在",于是那条 fail-safe 续期分支永远走不到。
_MISSING = object()

# `cause` 链的最大追溯深度。纯防环 / 防病态深链,正常链路只有 1~2 层。
_CAUSE_CHAIN_LIMIT = 16


class OwnerBeginOutcomeUnknown(Exception):
    """Begin 的服务端提交结果**无法判定**。Go: `errOwnerBeginOutcomeUnknown`。

    ★ 它只是进程内的控制流 cause,对客户端仍统一暴露 `ErrUnavailable`。
      调用方看到它必须**保留** allocation / Pod / 本批已写 owner,不能按普通失败
      回滚或清理 —— 否则可能留下一份指向已删除 Pod 的归属记录。
    """


# 与 Go 的 `var errOwnerBeginOutcomeUnknown = errors.New(...)` 同:模块级单例,
# 判定用 `isinstance` 而不是比字符串。
OWNER_BEGIN_OUTCOME_UNKNOWN = OwnerBeginOutcomeUnknown("owner Begin outcome unknown")


def is_owner_begin_outcome_unknown(exc: BaseException | None) -> bool:
    """沿 `PandoraError.cause` 链判定是否为"提交结果未知"。Go: `errors.Is(err, errOwnerBeginOutcomeUnknown)`。

    ★ 必须走链而不是只看最外层:`errcode.NewCause` 的语义就是"外层 code 给客户端、
      内层 cause 给控制流",只看最外层等于把这个区分丢掉,于是 outcome-unknown 会
      被当成普通失败去回滚 —— 正是本模块最不能发生的那一件事。
    """
    seen = 0
    cur = exc
    while cur is not None and seen < _CAUSE_CHAIN_LIMIT:
        if isinstance(cur, OwnerBeginOutcomeUnknown):
            return True
        cur = getattr(cur, "cause", None)
        seen += 1
    return False


class OwnerAuthority(Protocol):
    """owner 权威的调用面(Query / Begin / Admit / Release)。Go: `biz.OwnerAuthority`。

    由 `clients.GrpcOwnerLeaseRenewer` 实现(与租约续写共用一条连接);
    可为 None(未配 `allocator.owner_addr` = owner 服务未部署)。

    `begin_transition` 的返回值契约(owner.proto `BeginTransitionResponse`):
    真实写时其 `owner_epoch + operation_id` 是**精确回滚的唯一凭据**;同 target
    no-op 时是既有记录;EPOCH_CONFLICT 时(挂在异常的 `current_record` 上)是权威
    当前记录。
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

    async def release_owner(self, player_id: int, owner_epoch: int, operation_id: str) -> None: ...


@dataclasses.dataclass(frozen=True, slots=True)
class OwnerBeginGrant:
    """本次调用**已成功写进权威**的一份归属,用于部分失败时精确回滚。
    Go: `ownerBeginGrant`。

    ★ epoch / operation 必须取**权威回传的新记录**,不能自铸也不能推算:
      `ReleaseOwner` 要求两者与记录全等才生效,推算出来的值只会静默 no-op ——
      看起来回滚成功了,残留却还在。
    """

    player_id: int
    owner_epoch: int
    operation_id: str


def sweep_stale_owner_admitted(admitted: dict[str, float], cutoff: float) -> None:
    """删除 last-touch 早于 cutoff 的 census 准入缓存项。Go: `sweepStaleOwnerAdmitted`。

    ★ 值不是 float 的也删(理论不可达,所有写入点都写 float)—— 有界性不能建立在
      "理论上不会发生"之上:一个类型坏掉的值会让它**永远**通不过时间比较,那一项
      就成了永久驻留的泄漏点。fail-safe 删掉即可:玩家还在场的话下一轮 census 会
      Query→Admit 补回(至多一次多余往返)。

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
    """回传记录是否**逐格**等于本次 target。Go: `ownerRecordExactlyTargets`。

    这是滚动升级的硬门:旧 owner binary 可能仍把"同物理实例、不同 allocation"当成
    no-op —— RPC 返回成功,却把**旧** target 原样带回。只看错误码的话,调用方会拿着
    一份指向旧 allocation 的记录去交付 READY。

    ★ `instance_epoch` 这一格不能省。§9.22 要求 instance epoch 变化(实例代次翻转 /
      灾备接管)必须递增 `owner_epoch`;漏比它会把本应发生的 owner 迁移误当同目标跳过。
      (Go 侧这一格是后补的 —— hub_allocator 复审 P1-3 加了、battle 侧一直没加。)

    ★ `operation_id` 用 `placement.valid_operation_id` 而不是 `!= ""`:它是 §9.23 的
      端到端幂等键,非 canonical UUIDv4 的值意味着写者根本没按协议铸号,拿它去
      Release 只会静默 no-op。
    """
    return (
        rec.owner_epoch > 0
        and placement.valid_operation_id(rec.operation_id)
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
    attempt_budget_sec: float,
) -> tuple[OwnerRecordView, bool]:
    """把单个玩家的 owner 权威推进到 target。Go: `beginOnePlayer`。

    返回 `(权威最终记录, 本次调用是否真实创建)`。`created=False` 表示这份记录是**别人**
    写的(并发同 target no-op),调用方据此**绝不**把它纳入自己的 rollback。

    顺序:Query(取 CAS 期望值 expect_epoch) → Begin(带显式 UUIDv4 operation)。

    ★ Query 失败**必须**上抛,不能当成"没有归属"继续:那正是 §9.22 禁止的
      「冒充 OFFLINE」,直接后果是第二台 DS 被放进来。

    ★ 冲突**不按原 target 盲重试**:EPOCH_CONFLICT 可能恰恰说明更新的归属已经胜出,
      第二次写旧 target 会把 winner 回滚。强调用方重走整条分配链并重读 allocation。

    ★ 成功也不能只看"没抛异常":还要 `owner_record_exactly_targets` 逐格复核
      (滚动升级期旧 binary 的 no-op 回传,见该函数注释)。

    ★ 非 EPOCH_CONFLICT 失败要**独立预算回读**一次(Go 用 `plog.Detach` 剥掉请求级
      ctx;Python 侧靠"回读发生在 attempt 超时作用域**之外**"达到同一效果):
      exact + requested operation 证明本次创建;exact + 其他 operation 证明既有同
      target 已收敛;回读也失败 → `OwnerBeginOutcomeUnknown`。
    """
    requested_operation = placement.new_operation_id()
    begin_err: BaseException | None = None
    got: OwnerRecordView | None = None
    begin_attempted = False
    try:
        async with asyncio.timeout(attempt_budget_sec):
            # Query 失败直接穿透:一次 Begin 都没发出,不存在"可能已提交"。
            rec = await auth.query_owner(player_id)
            begin_attempted = True
            try:
                got = await auth.begin_transition(
                    player_id, rec.owner_epoch, requested_operation, owner_type, target
                )
            except asyncio.CancelledError:
                # ★ 必须紧邻宽 except 之上:取消是停机控制流,吞掉它会让本协程不退出。
                raise
            except BaseException as exc:  # noqa: BLE001 —— 下面按错误码分流
                if errcode.as_code(exc) == errcode.ErrOwnerEpochConflict:
                    # 冲突是 CAS 设计内的竞争,权威当前记录已挂在 exc.current_record 上。
                    raise
                begin_err = exc
    except TimeoutError as exc:
        if not begin_attempted:
            raise
        # 预算在 Begin 途中耗尽:与"回包丢失"同类 —— 服务端可能已 commit,交给回读判定。
        begin_err = exc

    if begin_err is None and got is not None:
        # `got is not None` 只是给类型收窄用:begin_err 为 None 就意味着
        # begin_transition 已正常返回一份记录,两者不可能同时成立地走到回读段。
        if not owner_record_exactly_targets(got, owner_type, target):
            err = errcode.PandoraError(
                errcode.ErrInvalidState,
                "owner Begin returned a non-exact target player=%d",
                player_id,
            )
            err.current_record = got
            raise err
        return got, got.operation_id == requested_operation

    # ── 独立预算回读(此处已在 attempt 超时作用域之外)──────────────────────
    try:
        async with asyncio.timeout(OWNER_BEGIN_READBACK_BUDGET_SEC):
            observed = await auth.query_owner(player_id)
    except asyncio.CancelledError:
        raise
    except BaseException as readback_err:  # noqa: BLE001 —— 回读不可达即"结果未知"
        raise errcode.PandoraError(
            errcode.ErrUnavailable,
            "owner Begin outcome unknown player=%d: begin=%s readback=%s",
            player_id,
            begin_err,
            readback_err,
            cause=OWNER_BEGIN_OUTCOME_UNKNOWN,
        ) from readback_err

    if owner_record_exactly_targets(observed, owner_type, target):
        return observed, observed.operation_id == requested_operation
    # 回读明确显示当前 owner 不是本次 target:无论 Begin 未提交,还是提交后已被更新的
    # writer 覆盖,本 allocation 都不再拥有该玩家,可按原错误正常补偿。
    raise begin_err


async def owner_verify_players_exact(
    auth: OwnerAuthority | None,
    players: list[int],
    owner_type: int,
    target: OwnerTargetView,
    budget_sec: float,
) -> None:
    """claim loser / 幂等重试交付 READY 前的**只读门**。Go: `ownerVerifyPlayersExact`。

    ★ loser 不能再跑 Begin:同一批玩家的多个并发 binder 会让一个调用把另一个调用
      已经依赖的 grant 当成自己的补偿对象 Release —— per-player 的 Begin/Release
      无法原子提交整批。这里只 Query,并要求 roster 里**每个**玩家都已 exact 指向
      winner 的完整 Battle target;任一缺失 / 漂移即 fail-closed,等待唯一 claim
      winner 完成 bind 或由外层重试。

    本函数零 Begin / 零 Release 副作用 —— 改这一点之前先读上面那段。
    """
    if auth is None or not players:
        return
    async with asyncio.timeout(budget_sec):
        for player_id in players:
            rec = await auth.query_owner(player_id)
            if not owner_record_exactly_targets(rec, owner_type, target):
                raise errcode.PandoraError(
                    errcode.ErrUnavailable,
                    "owner target not exact for ready delivery player=%d",
                    player_id,
                )


async def rollback_owner_begins(
    auth: OwnerAuthority, granted: list[OwnerBeginGrant]
) -> None:
    """精确撤销本次未交付的 Begin。Go: `rollbackOwnerBegins`。

    只撤销本批刚写进去的 `(player, epoch, operation)` 三元组,不动别的。记录若已被
    别的写者推进(玩家已被分到别处),epoch/operation 不再匹配,owner 侧按迟到调用
    幂等 no-op,不误伤。

    ★ **逆序释放**:与写入顺序相反,先撤最后写进去的。

    ★ best-effort:失败只告警不上抛。此时 owner 权威本就不可用,重试也写不进去,
      而把回滚的错误盖掉调用方的原始错误会让上层误判失败原因;下次分配的 CAS 覆盖
      仍是兜底。

    ⚠️ 与 Go 的差异:Go 用 `plog.Detach(ctx)` 让回滚**不被调用方取消穿透**;
      asyncio 没有等价物,进程停机时这批回滚可能来不及做完。残留由下次分配的 CAS
      覆盖兜底,与 Go 在"回滚全失败"时的处境相同。
    """
    if not granted:
        return
    failed = 0
    first_err: BaseException | None = None
    try:
        async with asyncio.timeout(OWNER_BEGIN_ROLLBACK_BUDGET_SEC):
            for grant in reversed(granted):
                try:
                    await auth.release_owner(
                        grant.player_id, grant.owner_epoch, grant.operation_id
                    )
                except asyncio.CancelledError:
                    raise
                except BaseException as exc:  # noqa: BLE001 —— best-effort,逐条计数
                    failed += 1
                    if first_err is None:
                        first_err = exc
    except TimeoutError as exc:
        # 预算耗尽:剩下没来得及撤的按失败计,让告警里的数字与实际残留一致。
        failed += 1
        if first_err is None:
            first_err = exc
    if failed > 0:
        plog.get().warning(
            "owner_begin_rollback_incomplete",
            granted=len(granted),
            release_failed=failed,
            first_err=str(first_err),
            hint="残留归属指向已回收实例;下次分配的 CAS 覆盖兜底,持续出现须人工核查",
        )
        return
    plog.get().info(
        "owner_begin_rolled_back",
        granted=len(granted),
        hint="已精确撤销本次未交付的归属写",
    )


async def owner_begin_players(
    auth: OwnerAuthority | None,
    players: list[int],
    owner_type: int,
    target: OwnerTargetView,
    budget_sec: float,
) -> None:
    """批量**强** Begin(contract 阶段):任一玩家写不进 owner 权威即整体失败。
    Go: `ownerBeginPlayers`。

    `auth is None` = `owner_addr` 未配置(owner 服务未部署),属部署形态问题,不在
    本函数收敛。

    ★ 超预算即失败,而**不是** migrate 阶段的"跳过剩余玩家":一局里部分玩家有归属、
      部分没有,比整局失败重来更难收敛,也会让 Admit 侧看到半截状态。

    ★ 可判定的整体失败必须连同**已写入的部分**一起撤销:本函数串行逐玩家写,失败点
      之前的玩家归属已经落进权威,而调用方紧接着就会把那台 Pod 删掉。不回滚就会留下
      一批"归属指向已删除实例"的 PENDING 记录,且没有任何路径能清掉它们
      (详见 `rollback_owner_begins` 与模块头)。回滚是 best-effort,**不改变**本函数
      抛出的原始错误。

    ★ 唯一例外是 `OwnerBeginOutcomeUnknown`:当前玩家可能已提交,而回读也不可达。
      此时既不回滚此前 grants,也要求调用方不清理 allocation/Pod。保留整批虽会
      fail-closed 暂停交付,但 owner 恢复后 claim loser 可只读验证全员 exact 后收敛;
      回滚半批再留下一个未知提交只会主动制造更难恢复的不一致。
    """
    if auth is None or not players:
        return
    deadline = time.monotonic() + budget_sec
    granted: list[OwnerBeginGrant] = []
    failure: BaseException | None = None
    failed_at = 0
    failed_player = 0
    for index, player_id in enumerate(players):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            # 预算耗尽 = 失败(等价于 Go 的 budgetCtx 已过期,RPC 立即返回 ctx 错误)。
            failure = TimeoutError("owner begin budget exhausted")
            failed_at, failed_player = index, player_id
            break
        try:
            rec, created = await begin_one_player(
                auth, player_id, owner_type, target, remaining
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001 —— fail-closed:任何失败都整体失败
            failure = exc
            failed_at, failed_player = index, player_id
            break
        if created:
            granted.append(
                OwnerBeginGrant(
                    player_id=player_id,
                    owner_epoch=rec.owner_epoch,
                    operation_id=rec.operation_id,
                )
            )
    if failure is None:
        return

    outcome_unknown = is_owner_begin_outcome_unknown(failure)
    plog.get().warning(
        "owner_begin_failed",
        players=len(players),
        failed_at=failed_at,
        player_id=failed_player,
        err=str(failure),
        pod=target.pod_name,
        instance_uid=target.instance_uid,
        granted_before_failure=len(granted),
        outcome_unknown=outcome_unknown,
        hint=(
            "contract 强依赖:归属未全量定案即拒绝交付;"
            "outcome unknown 必须保留 grants/allocation/pod"
        ),
    )
    if outcome_unknown:
        raise failure
    await rollback_owner_begins(auth, granted)
    raise failure


async def owner_release_abandoned_players_weak(
    auth: OwnerAuthority | None,
    players: list[int],
    self_pod: str,
    self_uid: str,
    budget_sec: float,
) -> None:
    """判弃对局后释放仍指向本实例的 owner 记录。Go: `ownerReleaseAbandonedPlayersWeak`
    (INC-20260729-002 P0-B1;弱依赖)。

    为什么必须有:§9.4 的 abandoned 补偿链原本被当作「玩家解放出口」,但它只做了
    lifecycle 事件 + battle_result 记账 + match 释放,**从没动过 owner 权威**。后果是
    判弃后的恢复查询会一直返回 TARGET(指向已删除的 battle Pod),客户端按 §9.23 反复
    Travel 到一个不存在的实例 —— 比不接 owner 更糟。释放后恢复查询才会落到
    「无归属 → 首次进场链 → Hub」。

    安全边界(三条,缺一不可):

        ① **时序**    只能在被判弃实例的 GameServer 回收**已确认之后**调用(与
                      deliverAbandoned 同一门控)。提前释放会在旧 DS 可能仍在跑时
                      放行新归属 = 双 DS(§9.22)。
        ② **exact 身份** 只释放"记录仍指向本次被判弃的 pod+uid 且类型为 BATTLE"的玩家。
                      玩家已被迁到新 DS(epoch 已推进、pod/uid 已变)时必须跳过,
                      否则误删活归属。
        ③ **compare-delete** 带 Query 读到的 `owner_epoch + operation_id` 调用,
                      owner 侧按 epoch 比对拒绝陈旧释放。

    失败只告警:owner 未启用 / 抖动时,login 侧 `InspectBattleRoute` 的
    abandoned →(过再入屏障)→ Terminal → Hub 旧门仍能让玩家收敛。
    """
    if auth is None or not players or self_pod == "" or self_uid == "":
        return
    deadline = time.monotonic() + budget_sec
    query_failed = 0
    release_failed = 0
    released = 0
    skipped = 0
    first_err: BaseException | None = None
    sample_player = 0

    def note_fail(player_id: int, exc: BaseException) -> None:
        nonlocal first_err, sample_player
        if first_err is None:
            first_err, sample_player = exc, player_id

    try:
        for index, player_id in enumerate(players):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                plog.get().warning(
                    "owner_release_abandoned_budget_exhausted",
                    players=len(players),
                    done=index,
                    remaining_players=len(players) - index,
                    hint="migrate 弱依赖;login InspectBattleRoute 旧门仍能收敛",
                )
                return
            try:
                async with asyncio.timeout(remaining):
                    rec = await auth.query_owner(player_id)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001 —— 弱依赖:计数后继续下一个玩家
                query_failed += 1
                note_fail(player_id, exc)
                continue

            # ② exact 身份门:只有记录仍指向本次被判弃的实例才允许释放。
            if (
                rec.owner_type != OWNER_TYPE_BATTLE
                or rec.pod_name != self_pod
                or rec.instance_uid != self_uid
            ):
                skipped += 1
                continue

            # ③ compare-delete:epoch/operation 取自刚读到的记录,owner 侧再校验一次。
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                plog.get().warning(
                    "owner_release_abandoned_budget_exhausted",
                    players=len(players),
                    done=index,
                    remaining_players=len(players) - index,
                    hint="migrate 弱依赖;login InspectBattleRoute 旧门仍能收敛",
                )
                return
            try:
                async with asyncio.timeout(remaining):
                    await auth.release_owner(player_id, rec.owner_epoch, rec.operation_id)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001 —— 弱依赖:计数后继续
                release_failed += 1
                note_fail(player_id, exc)
                continue
            released += 1
    finally:
        # 对应 Go 的 defer:**成功条数也要可观测**。判弃是低频事件,一条汇总不会刷屏,
        # 而"释放了几个 / 跳过几个"正是排查「玩家为什么还回不去 Hub」时第一眼要看的数。
        plog.get().info(
            "owner_release_abandoned_weak",
            players=len(players),
            released=released,
            skipped_not_self=skipped,
            query_failed=query_failed,
            release_failed=release_failed,
            pod=self_pod,
            sample_player_id=sample_player,
            first_err=str(first_err),
        )


async def owner_admit_census_weak(
    auth: OwnerAuthority | None,
    admitted: dict[str, float],
    players: list[int],
    owner_type: int,
    self_pod: str,
    self_uid: str,
    budget_sec: float,
) -> None:
    """授权心跳 census 首见玩家代提交 Admit。Go: `ownerAdmitCensusWeak`(migrate 近似;弱依赖)。

    近似在哪:census 来自**绑定 exact 实例身份的授权心跳**,是"该实例正在服务该玩家"
    的证据,但不是 DS Admission 链的原生提交点。DS 原生提交上线后本近似退役。
    弱依赖是刻意的:一个玩家的自愈失败不该打挂整台 DS 的心跳。

    只有记录确实指向本实例(pod+uid 同 && 类型同 && PENDING)才 Admit;target 取
    **记录自身**的字段(exact 全等校验由 owner 侧执行,pod/uid 是本调用方独立断言的
    那部分)。屏障未开(`retry_after_ms > 0`)→ 本轮跳过,下次心跳重试,**不告警**
    (否则每次正常迁移都刷一片假告警)。

    缓存有界(§9.18,对齐 hub_allocator):
      - 值存 last-touch(单调秒):命中即续期(接近过期才写,降写频),活实例项恒新鲜;
        仅已销毁 Battle 实例(UID 不再心跳续期)的项会老化超 TTL,由后台
        `sweep_stale_owner_admitted` 清除。
      - 按本实例 census **剪枝**:玩家离开本实例(不再出现在 census)即删其缓存项,
        与 TTL 兜底互补(TTL 清死实例项,剪枝清活实例上已离场玩家项)。
    """
    if auth is None:
        return

    # ★ 剪枝必须在"本轮无玩家就早退"**之前**:最后一名玩家离场时 census 为空,若此时
    #   早退,他的 admitted 项就永久残留;等他回流本实例(owner epoch 已推进、新 PENDING)
    #   时会被缓存命中误吞,跳过 Query→Admit —— 新纪元的 Admit 永远不会提交。
    prefix = self_uid + "|"
    present = {f"{prefix}{player_id}" for player_id in players}
    for key in [k for k in admitted if k.startswith(prefix) and k not in present]:
        admitted.pop(key, None)
    if not players:
        return  # 剪枝已完成;本轮无玩家可代提交 Admit。

    deadline = time.monotonic() + budget_sec
    query_failed = 0
    admit_failed = 0
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
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                # 预算耗尽:剩余玩家下次心跳继续(census 每 ~5s 一轮,自然收敛)。
                return

            try:
                async with asyncio.timeout(remaining):
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
                # 记录不指向本实例(迁移中 / 漂移),不是本实例可断言的准入。
                continue
            if rec.phase == OWNER_PHASE_ADMITTED:
                admitted[key] = now
                continue
            if rec.phase != OWNER_PHASE_PENDING:
                continue

            target = OwnerTargetView(
                pod_name=rec.pod_name,
                instance_uid=rec.instance_uid,
                instance_epoch=rec.instance_epoch,
                assignment_or_allocation_id=rec.assignment_or_allocation_id,
                release_track=rec.release_track,
            )
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            try:
                async with asyncio.timeout(remaining):
                    await auth.admit(player_id, rec.owner_epoch, rec.operation_id, target)
                admitted[key] = now
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001 —— 弱依赖:分流成 WAIT 或失败计数
                retry_after = exc.retry_after_ms if isinstance(exc, errcode.PandoraError) else 0
                if retry_after > 0:
                    # 屏障未开:预期中的 WAIT(旧 DS 租约还没过安全截止),下次心跳重试。
                    continue
                admit_failed += 1
                note_fail(player_id, exc)
    finally:
        # 对应 Go 的 defer:预算耗尽提前 return 时同样要把这轮的失败汇总打出来。
        # 模式 C:本函数由**每次心跳**(每 ~5s/对局)对全部在场玩家调用,owner 抖动时
        # 逐玩家打 Warn = 并发对局数 × 玩家数 / 5s 条刷屏,故批末只汇总一条。
        if query_failed + admit_failed > 0:
            plog.get().warning(
                "owner_admit_census_weak_failed",
                players=len(players),
                query_failed=query_failed,
                admit_failed=admit_failed,
                sample_player_id=sample_player,
                first_err=str(first_err),
                hint="migrate 弱依赖,再入屏障双门兜底",
            )
