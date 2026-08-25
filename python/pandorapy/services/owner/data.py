"""owner 权威数据层 —— 对应 Go 侧 internal/data/owner_repo.go(pandora_owner 库)。

一致性核心(§9.22):
    owner_record 行是**每玩家的串行化锚点**。所有 transition 先 `SELECT ... FOR UPDATE`
    锁该行,然后 epoch 单调 CAS、admit_not_before 计算(同事务 FOR UPDATE 读旧实例租约行,
    取 CAS 线性化点观察值)、PENDING→ADMITTED 推进,**全部在同一事务内完成**。

    §9.22 明确禁止「owner 放 MySQL、准入 lease / 屏障放 Redis 或 etcd 后再跨存储先查后写」——
    那样 CAS 线性化点与屏障计算不在同一个一致性域,脑裂窗口重新打开。

锁序固定 `owner_record → ds_instance_lease`,Renew 只锁 lease 行 —— 无环无死锁。

SQL 写法 TiDB 安全:只锁存在行 + 条件更新,**不依赖间隙锁**(TiDB 无 gap 锁,
`FOR UPDATE` 在零行时不加锁,所以所有临界区都必须先有守卫行可锁)。
"""

from __future__ import annotations

import dataclasses
import time
from typing import Protocol

from pandorapy import errcode, placement
from pandorapy import log as plog

# OwnerType(对齐 owner.proto OwnerType)。
OWNER_TYPE_NONE = 0
OWNER_TYPE_HUB = 1
OWNER_TYPE_BATTLE = 2

# OwnerPhase(对齐 owner.proto OwnerPhase)。
OWNER_PHASE_NONE = 0
OWNER_PHASE_PENDING = 1
OWNER_PHASE_ADMITTED = 2

# transition 审计 op。
TRANSITION_OP_BEGIN = 1
TRANSITION_OP_ADMIT = 2
TRANSITION_OP_RELEASE = 3

# owner_transition_log.detail 列宽(VARCHAR(512))。
#
# ★ 必须在应用侧钳制:sql_mode 含 STRICT_TRANS_TABLES(§9.24),超长写入是 Error 1406
# 而不是截断 —— 那会让一次**本该成功的 owner 迁移**因为审计字段太长而整事务失败。
# 审计流水缺一截可以接受,玩家进不去场景不可以。
TRANSITION_DETAIL_MAX_LEN = 512

# 「屏障未开」从 debug 升 info 的剩余等待阈值(毫秒)。
#
# 不整条 debug:线上默认 info 级,整条 debug =「玩家匹配好了却进不去」最常见的落点
# 永远查不到。也不能整条 info:调用方按 wait_ms 轮询,BATTLE→* 的屏障可长达 ~27s,
# 短剩余量的收尾轮询会刷屏。取 5s:HUB 旧 owner 分支屏障恒为 0(不触发),
# BATTLE 分支的实质等待必然远超它。
BARRIER_WAIT_INFO_THRESHOLD_MS = 5000


def now_ms() -> int:
    return int(time.time() * 1000)


@dataclasses.dataclass(frozen=True, slots=True)
class OwnerTarget:
    """exact DS 实例身份。同名 Pod 重建后 instance_uid / instance_epoch 不同,
    因此**永远不会** Equal —— 这是 §9.22 exact 绑定的基础。"""

    pod_name: str = ""
    instance_uid: str = ""
    instance_epoch: int = 0
    assignment_or_allocation_id: str = ""
    release_track: str = ""

    def complete(self) -> bool:
        """实例身份完整性(pod/uid/epoch/分配 ID/轨道全非空)。"""
        return (
            bool(self.pod_name.strip())
            and bool(self.instance_uid.strip())
            and self.instance_epoch > 0
            and bool(self.assignment_or_allocation_id.strip())
            and bool(self.release_track.strip())
        )


@dataclasses.dataclass(frozen=True, slots=True)
class OwnerRecord:
    """每玩家 owner 权威记录。lease_deadline_ms 是派生字段(同事务读实例租约)。"""

    player_id: int = 0
    owner_epoch: int = 0
    owner_type: int = OWNER_TYPE_NONE
    phase: int = OWNER_PHASE_NONE
    target: OwnerTarget = dataclasses.field(default_factory=OwnerTarget)
    operation_id: str = ""
    admit_not_before_ms: int = 0
    lease_deadline_ms: int = 0
    updated_at_ms: int = 0

    # 该玩家的 **Hub 来源版本高水位**(INC-20260818-003)。
    #
    # 与 owner_epoch 是两个不同维度,别混:
    #   - owner_epoch        回答「谁**后**提交」—— 由 Owner 自己在 CAS 时 +1
    #   - hub_source_revision 回答「谁的**来源**更新」—— 由 hub_allocator 在真正改变
    #     target 的 assignment CAS 上领号,Owner 只负责比较与持久化
    # 事故反例里旧 binary 恰好能拿到**合法的** expect_epoch(它先 Begin 后 CAS),
    # 所以只靠 epoch 挡不住它;能挡住的只有来源版本。
    #
    # ★ **只前进,永不清零**:Release 与 BATTLE 迁移都不动它。清零等于「打完一局回大厅」
    # 就把门重新对 legacy(0)敞开,滚动窗口里的旧写者随即又能写进来。
    hub_source_revision: int = 0


def transition_detail(target: OwnerTarget, admit_not_before_ms: int, from_pod: str = "") -> str:
    """把一次迁移的 exact 实例身份编码进审计流水的 detail 列。

    ★ 为什么不能只写 pod_name:Agones 下 Pod 名会被复用,同名 Pod 重建后两行 detail
    完全一样;而 locator 侧的 join key 是 assignment_id、allocator 侧是
    allocation_id / match_id —— 只有 pod 名时,「这次 owner 迁移对应哪次 hub assignment /
    哪局对局」永远接不上。admit_not_before_ms 则是屏障时刻的唯一持久证据。

    格式是给人读的 key=value 串,**没有任何读取方**,因此不构成对外契约;
    新增字段只往后追加。
    """
    s = (
        f"pod={target.pod_name} uid={target.instance_uid} "
        f"iepoch={target.instance_epoch} aid={target.assignment_or_allocation_id} "
        f"track={target.release_track} anb={admit_not_before_ms}"
    )
    if from_pod:
        s += f" from_pod={from_pod}"
    return s[:TRANSITION_DETAIL_MAX_LEN]


def compute_admit_not_before_ms(
    old_owner_type: int,
    old_lease_deadline_ms: int,
    now: int,
    skew_margin_seconds: int = placement.DS_FENCE_SKEW_MARGIN_SECONDS,
) -> int:
    """计算准入屏障时刻。★ 这是 §9.22 核心时序不等式的兑现点。

    分流依据是**旧 owner 的类型**:

      旧 owner = BATTLE → max(now, 旧 lease 截止) + 余量
          对局 DS 可能已失联但仍在跑(玩家还能操作、还能产生业务写)。必须等它的
          租约确定过期再加时钟/网络余量,才能让新 DS 开始可玩。
          这条保证:旧 DS 最晚停止可玩时间 < 新 DS 最早开始可玩时间。

      旧 owner = HUB 或无 → now(屏障不等待)
          Hub 是协作迁移:双写由 epoch fencing 拦(旧 epoch 的写一律拒),
          双可玩由客户端单连接拆链拦(客户端只有一条连接,Travel 走了就断了旧的)。
          这里等待没有收益,只会让每次进大厅都卡 27 秒。

    ⚠️ 余量必须**加在取 max 之后**,不能只加在 lease 上 —— 若 lease 已过期,
    max 取的是 now,此时不加余量等于零屏障。
    """
    margin_ms = skew_margin_seconds * 1000
    if old_owner_type == OWNER_TYPE_BATTLE:
        return max(now, old_lease_deadline_ms) + margin_ms
    return now


def release_noop_reason(
    found: bool, rec: "OwnerRecord", owner_epoch: int, operation_id: str
) -> str:
    """迟到 Release 被判 no-op 的具体理由 —— 对应 Go 的 releaseNoopReason。

    理由必须逐项分开,不能只打一句"no-op":四种成因的处置完全不同 ——
    already_released 是正常重放,epoch_mismatch / operation_mismatch 说明调用方
    拿着过期上下文在释放(可能是另一条链的残留),record_absent 则是权威侧丢了记录。
    """
    if not found:
        return "record_absent"
    if rec.owner_epoch != owner_epoch:
        return "epoch_mismatch"
    if rec.operation_id != operation_id:
        return "operation_mismatch"
    if rec.owner_type == OWNER_TYPE_NONE:
        return "already_released"
    return "unknown"


def admit_mismatch_reason(
    found: bool,
    rec: OwnerRecord,
    owner_epoch: int,
    operation_id: str,
    target: OwnerTarget,
) -> str:
    """把 Admit 那个「任一项不匹配都拒」的合取条件拆成**单一枚举 reason**。

    §11.3 R2:一个 if 收敛了 N 个条件的,必须拆成 N 个 reason —— 否则线上只看到
    「准入被拒」,不知道是 epoch 老了、operation 换了、还是打到了别的实例。
    判定顺序与 Go 侧 if 内一致。
    """
    if not found:
        return "owner_record_absent"
    if rec.owner_epoch != owner_epoch:
        return "owner_epoch_mismatch"
    if rec.operation_id != operation_id:
        return "operation_id_mismatch"
    if rec.target != target:
        return "target_instance_mismatch"
    if rec.phase not in (OWNER_PHASE_PENDING, OWNER_PHASE_ADMITTED):
        return "phase_not_admittable"
    return ""


class OwnerRepo(Protocol):
    """owner 权威数据层抽象。"""

    async def query(self, player_id: int) -> OwnerRecord: ...
    async def begin_transition(
        self,
        player_id: int,
        expect_epoch: int,
        operation_id: str,
        owner_type: int,
        target: OwnerTarget,
        source_revision: int,
        skew_margin_seconds: int,
    ) -> OwnerRecord: ...
    async def admit(
        self, player_id: int, owner_epoch: int, operation_id: str, target: OwnerTarget
    ) -> tuple[OwnerRecord, int]: ...
    async def renew_instance_lease(self, target: OwnerTarget, lease_seconds: int) -> int: ...
    async def release(
        self, player_id: int, owner_epoch: int, operation_id: str, skew_margin_seconds: int
    ) -> OwnerRecord: ...
    async def sweep_transition_log(self, retention_days: int, batch: int) -> int: ...


def barrier_wait_ms(rec: OwnerRecord, now: int) -> int:
    """屏障剩余等待毫秒。<=0 表示已开。"""
    return max(0, rec.admit_not_before_ms - now)


def log_barrier_not_open(player_id: int, rec: OwnerRecord, wait_ms: int) -> None:
    """屏障未开的分级日志 —— 见 BARRIER_WAIT_INFO_THRESHOLD_MS 的取值理由。"""
    logger = plog.get()
    fields = {
        "player_id": player_id,
        "owner_epoch": rec.owner_epoch,
        "operation_id": rec.operation_id,
        "wait_ms": wait_ms,
        "admit_not_before_ms": rec.admit_not_before_ms,
    }
    if wait_ms >= BARRIER_WAIT_INFO_THRESHOLD_MS:
        logger.info("owner_admit_barrier_not_open", **fields)
    else:
        logger.debug("owner_admit_barrier_not_open", **fields)


def barrier_not_open_error(
    wait_ms: int, record: "OwnerRecord | None" = None
) -> errcode.PandoraError:
    """屏障未开 —— 调用方按 retry_after 退避重查,**保留 session 与原 operation_id**。

    §9.23:脑裂时安全优先但不能永久卡流程 —— 返回带明确原因和 retry_after 的 WAIT,
    由同一 coordinator 的 watchdog 到期重查,不能等旧 DS 某个可能永不到达的回调。

    ★ wait_ms 与当前记录必须**随错误一起**带出去。Go 侧 Admit 的签名是
    `(rec, retryAfterMs, err)` 三元,三样一起返回;Python 用异常传播,不显式挂上
    就等于丢掉 —— 调用方收到 `retry_after_ms=0` 的 WAIT,只能空转或干等,
    §9.23「不得无出口等待」当场被打穿,而且**没有任何报错**。
    """
    err = errcode.PandoraError(
        errcode.ErrOwnerBarrierNotOpen,
        "admission barrier not open, retry after %dms",
        wait_ms,
    )
    err.retry_after_ms = wait_ms
    err.current_record = record
    return err
