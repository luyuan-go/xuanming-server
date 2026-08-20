"""Hub assignment 来源版本 —— 对应 Go 侧 pkg/placement/source_revision.go(INC-20260818-003)。

# 解决的问题

滚动升级期新旧 hub_allocator 副本共存时,Owner 无法判断两份「target 不同」的 Begin
哪一份来自**更新的 assignment**。事故反例是确定性的:旧 binary 在 assignment CAS
**之前**执行 Owner Begin,conflict 后不复核当前 assignment 就拿新 epoch 盲写旧 target,
于是 Redis=B 而 Owner=A2。

`owner_epoch` 只能证明「谁**后**提交」,证明不了「谁的**来源**更新」—— 这是两个维度。

# 为什么现有字段都不行(事故文档已逐个否掉,别再试)

    assignment_id / auth_jti   随机 UUID:唯一但**不可排序**
    assigned_at_ms             墙钟:回拨与同毫秒碰撞都破坏全序
    auth_epoch / auth_gen      per-pod 凭据版本:跨 Pod 无序
    writer_token               只按 allocator **任期**递增 —— 而事故反例里
                               R1/R2 恰恰**同任期**
    owner_epoch                提交后的版本,倒果为因

# 编码

    source revision = 写者任期(高 40 位) × 2^24 + 任期内序号(低 24 位)

    ┌──────────────── 40 bit ────────────────┬───── 24 bit ─────┐
    │ writer term(etcd leader CreateRevision)│ 任期内严格递增序号 │
    └────────────────────────────────────────┴──────────────────┘

全序从两段各自的单调性推出:
  - 高位:writerlease 的 token = 本届 leader key 的 CreateRevision,
    etcd 保证**历届严格递增**;
  - 低位:同一任期只有一个写者进程在铸号,进程内自增即严格递增。

★ **不需要额外的持久发号器**:任期号本身已经持久(etcd revision 单调不回退),
进程崩溃重启会拿到**更大**的任期号,低位从 0 重来也不会与旧任期的号相撞。

★ 两边溢出都 **fail-closed**(返回错误),绝不静默回绕 ——
回绕会让一个旧来源看起来更新,那正是本机制要防的事。
"""

from __future__ import annotations

from pandorapy import errcode

# 低位(任期内序号)位宽。
SEQ_BITS = 24
# 单个写者任期内可铸的最大序号(含)。超出即 fail-closed。
MAX_SEQ = (1 << SEQ_BITS) - 1
# 可编码的最大写者任期号(含)。超出即 fail-closed。
MAX_TERM = (1 << (64 - SEQ_BITS)) - 1

# 「本次 Begin 来自尚未滚上本协议的旧写者」的哨兵值。
#
# ★ 0 不是「最小的合法版本」而是「**没有版本**」:它与任何非零 revision 都**不可比**。
# 一旦某玩家见过非零 revision,该玩家**永久拒绝** legacy ——
# 否则旧写者可以靠「我不带版本」绕过整道门。
LEGACY = 0


def compose(writer_term: int, seq: int) -> int:
    """把 (写者任期, 任期内序号) 编成一个可全序比较的 revision。

    seq 必须由调用方在**同一任期内严格递增**地给出,**从 1 开始**
    (0 留给 legacy 哨兵,故 seq=0 非法)。

    term = 0 表示写者未持有租约,此时根本不该铸号 ——
    没有任期就没有全序,铸出来的号无法与他人比较。
    """
    if writer_term == 0:
        raise errcode.PandoraError(
            errcode.ErrInvalidState,
            "source revision needs a writer term; term=0 means this replica holds no writer lease",
        )
    if writer_term > MAX_TERM:
        raise errcode.PandoraError(
            errcode.ErrInvalidState,
            "writer term %d exceeds source revision encoding limit %d",
            writer_term,
            MAX_TERM,
        )
    if seq == 0:
        raise errcode.PandoraError(
            errcode.ErrInvalidState,
            "source revision seq must start at 1; 0 is reserved for the legacy sentinel",
        )
    if seq > MAX_SEQ:
        raise errcode.PandoraError(
            errcode.ErrInvalidState,
            "source revision seq %d exhausted for term %d (max %d); "
            "step down and re-elect to get a fresh term",
            seq,
            writer_term,
            MAX_SEQ,
        )
    return (writer_term << SEQ_BITS) | seq


def split(revision: int) -> tuple[int, int]:
    """拆回 (任期, 序号)。

    ★ **只用于日志与排障** —— 判定新旧一律直接比 revision 本身,不要拆开来比:
    拆开比会引入「同任期时才比 seq」这类分支,而全序本来就不需要分支。
    """
    return revision >> SEQ_BITS, revision & MAX_SEQ


class Minter:
    """任期内序号铸号器。

    ★ 进程内计数就够,不需要持久发号器 —— 见模块头。
    任期变了就把序号归零(新任期的高位更大,全序仍然成立)。
    """

    __slots__ = ("_term", "_seq")

    def __init__(self) -> None:
        self._term = 0
        self._seq = 0

    def next(self, term: int) -> int:
        """领取本任期的下一个号。"""
        if term != self._term:
            self._term = term
            self._seq = 0
        self._seq += 1
        return compose(term, self._seq)


# ── Owner 侧的判定 ──────────────────────────────────────────────────────────

# 判定结果。★ 与 Go 的 classifySourceRevision 逐格对应,名字也照抄 ——
# 这些字符串会进日志,两栈用同一套词表运维才能用同一条 LogQL 查。
CLASSIFY_ACCEPT = "advances_high_water"
CLASSIFY_STALE = "older_than_high_water"
CLASSIFY_LEGACY_ACCEPTED = "legacy_compat_window"
CLASSIFY_LEGACY_REJECTED = "legacy_after_versioned"
CLASSIFY_LEGACY_REJECTED_GLOBALLY = "legacy_rejected_globally"
# 同一版本号 + 同一 target = 同一来源的重复投递(at-least-once),幂等放行、不推进。
CLASSIFY_SAME_REVISION_SAME_TARGET = "same_revision_same_target"
# ★ 同一版本号却指向**不同** target = 铸号被复制(两个写者共用了同一个任期)。
# 这不是"旧写者迟到",是全序前提本身被打破 —— 必须拒 + 告警。
CLASSIFY_SAME_REVISION_REUSED = "same_revision_different_target"

# 放行(可以继续走后面的 epoch CAS)的判定集合。
_ALLOWED = frozenset(
    {CLASSIFY_ACCEPT, CLASSIFY_LEGACY_ACCEPTED, CLASSIFY_SAME_REVISION_SAME_TARGET}
)
# 需要推进 high-water 的判定集合(只有唯一的正常前进路径)。
_ADVANCES = frozenset({CLASSIFY_ACCEPT})


def is_allowed(decision: str) -> bool:
    """该判定是否放行。**放行 ≠ 一定会写** —— 后面还有 epoch CAS 与各条 no-op 分支。"""
    return decision in _ALLOWED


def advances_high_water(decision: str) -> bool:
    """该判定是否应推进 high-water。"""
    return decision in _ADVANCES


def classify(
    incoming: int,
    high_water: int,
    *,
    same_target: bool = False,
    reject_legacy_globally: bool = False,
) -> str:
    """判定一次 Begin 携带的来源版本。顺序即优先级,与 Go 的 classifySourceRevision 逐格一致。

        incoming=0 且 reject_legacy_globally  → 拒:旧写者已宣称排空,不该再有 legacy 请求
        incoming=0 且 high_water>0            → 拒:该玩家见过版本,legacy 永久出局
        incoming=0 且 high_water=0            → 放行、不推进:兼容窗内的正常旧写者
        incoming<high_water                   → 拒:来源更旧(事故反例里迟到的 R1/R2 落这)
        incoming=high_water 且 same_target    → 放行、不推进:同一来源的重复投递,幂等
        incoming=high_water 且 !same_target   → 拒:同一版本号不可能产出两个 target,
                                                  出现即说明铸号被复制(两个写者共用同一任期)
        incoming>high_water                   → 放行并推进:唯一的正常前进路径

    ★ legacy 那三行是整道门的关键:0 与任何非零**不可比**,不能当成"最小值"放行 ——
    否则旧写者靠「我不带版本」就能绕过整道门,那正是 INC-20260818-003 的形状。

    ★ `incoming == high_water` 必须按 target 分成两格。合成一格的两种写法都会出事:
    一律放行 → 两个共用任期的写者互相覆盖;一律拒 → at-least-once 的重复投递被判 stale,
    迁移永远完不成。
    """
    if incoming == LEGACY:
        if reject_legacy_globally:
            return CLASSIFY_LEGACY_REJECTED_GLOBALLY
        # ★ 见过非零就永久拒 legacy(逐玩家自动生效,不依赖任何开关)。
        return CLASSIFY_LEGACY_ACCEPTED if high_water == LEGACY else CLASSIFY_LEGACY_REJECTED
    if incoming < high_water:
        return CLASSIFY_STALE
    if incoming == high_water:
        return CLASSIFY_SAME_REVISION_SAME_TARGET if same_target else CLASSIFY_SAME_REVISION_REUSED
    return CLASSIFY_ACCEPT
