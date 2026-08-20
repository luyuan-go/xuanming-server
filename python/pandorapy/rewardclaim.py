"""领奖记录位图 —— 对应 Go 侧 pkg/rewardclaim/rewardclaim.go(逐条同语义)。

用变长位图记录每个奖励档位是否已领取,判重 O(1)、落地紧凑。两类记录生命周期不同:

  - 永久类(签到里程碑 / 成就 / 新手 / 永久任务……):按「来源名」各存一条位图,
    只增不删,bit 位永久稳定;
  - 活动类:按「活动实例 ID」(每期一个新 ID,含轮次 / 版本)各存一条位图;活动下线时
    erase_activity 删整条,下期新活动用新 ID 从零开始 —— 即使复用了相同的档位 bit
    也不会串味,因为是另一条独立位图。

★ 落地形态就是位图的原始 bytes。snapshot() / load() 直接吐出 / 吃入
  {来源名: bytes} + {活动实例ID: bytes},调用方填进自己的 proto 存储 record ——
  不产生与 proto 漂移的并行结构。

★ **字节序必须与 Go 逐位一致**:同一条 record 会被 Go 副本与 Python 副本交替读写
  (灰度期两个实现并存,同一张 player_reward_claims 表)。bit i 落在
  `bits[i >> 3]` 的第 `i & 7` 位(低位在前),trimmed 去掉尾部全零 —— 任何一处
  取反都会让另一个实现把「已领」读成「未领」,玩家重复领奖且零报错。

并发:本类型非线程安全。它是"每玩家一份"的存档态,由持有方串行访问(player 侧靠
乐观锁 version 兜住跨请求并发)。
"""

from __future__ import annotations

from pandorapy import errcode

# MAX_BIT_INDEX 是单条位图允许的 bit 索引安全上界(1,048,576 位 = 128 KiB)。
# 防止恶意 / 错误传入的超大 index 把位图撑到撑爆内存。业务真实档位数远小于此。
MAX_BIT_INDEX = 1 << 20

# ── 条目数上限(§9 不变量 18 在 blob 内部的对应物)────────────────────────────
#
# MAX_BIT_INDEX 只约束**单条位图多大**,不约束**有多少条位图**。二者是两个独立的
# 失控方向。Record 整条序列化后落进 player_reward_claims.record(LONGBLOB,4GB),
# DB 层等于不设防;而 ClaimReward 是**客户端可直调**的 RPC,source /
# activity_instance_id 又没有配置表白名单 —— 于是客户端每换一个 source 字符串
# 就永久新增一条位图。每条最大 128 KiB,约 3.3 万次调用即可把单个玩家的 record
# 撑到 4 GB;且每次领奖都要全量 load→解码→snapshot→编码→save。
MAX_PERMANENT_SOURCES = 64
MAX_ACTIVITY_INSTANCES = 256
# 来源名是配置表里的短标识符,按**字节**长度限制(与 Go 的 len(string) 同口径:
# 按字符数判会让一个 64 字的中文来源名通过 Python 侧、被 Go 侧拒掉)。
MAX_SOURCE_NAME_LEN = 64


class RewardClaimError(errcode.PandoraError):
    """领奖位图操作失败的基类。code 由子类给,便于 biz 层直接分支。"""


class AlreadyClaimedError(RewardClaimError):
    """该档位此前已领取(幂等保护)。"""

    def __init__(self) -> None:
        super().__init__(errcode.ErrRewardAlreadyClaimed, "rewardclaim: 该奖励档位已领取")


class IndexTooLargeError(RewardClaimError):
    """bit 索引超出 MAX_BIT_INDEX 安全上界。"""

    def __init__(self) -> None:
        super().__init__(errcode.ErrRewardUnknownID, "rewardclaim: bit 索引超出安全上界")


class TooManyEntriesError(RewardClaimError):
    """记录里的位图条目数已达上限,拒绝**新增**条目(已存在的条目不受影响)。"""

    def __init__(self) -> None:
        super().__init__(errcode.ErrRewardUnknownID, "rewardclaim: 领奖记录条目数超出上限")


class SourceNameTooLongError(RewardClaimError):
    """永久来源名超长。"""

    def __init__(self) -> None:
        super().__init__(errcode.ErrRewardUnknownID, "rewardclaim: 永久来源名超出长度上限")


def _trimmed(src: bytes | bytearray | None) -> bytes | None:
    """去掉尾部全零字节;全零 / 空 → None(落地最小化,与 Go 的 trimmed 同)。"""
    if not src:
        return None
    end = len(src)
    while end > 0 and src[end - 1] == 0:
        end -= 1
    if end == 0:
        return None
    return bytes(src[:end])


class _Bitmap:
    """变长位图(Go 版 dynamic_bitset)。原始字节即落地形态,无需额外长度字段。"""

    __slots__ = ("bits",)

    def __init__(self, bits: bytes | bytearray = b"") -> None:
        self.bits = bytearray(bits)

    def test(self, i: int) -> bool:
        """索引 i 是否置位;越界(未分配)视为未置位。"""
        byte_idx = i >> 3
        if byte_idx >= len(self.bits):
            return False
        return self.bits[byte_idx] & (1 << (i & 7)) != 0

    def set(self, i: int) -> bool:
        """置位索引 i,必要时按需扩容。超上界返回 False。"""
        if i >= MAX_BIT_INDEX:
            return False
        byte_idx = i >> 3
        if byte_idx >= len(self.bits):
            self.bits.extend(b"\x00" * (byte_idx + 1 - len(self.bits)))
        self.bits[byte_idx] |= 1 << (i & 7)
        return True

    def count(self) -> int:
        return sum(bin(b).count("1") for b in self.bits)

    def set_indices(self) -> list[int]:
        """所有已置位的 bit 索引(升序)。供组装客户端可见的"已领取列表"。"""
        out: list[int] = []
        for byte_idx, by in enumerate(self.bits):
            bit = 0
            while by:
                if by & 1:
                    out.append(byte_idx * 8 + bit)
                by >>= 1
                bit += 1
        return out


class Record:
    """一名玩家完整的领奖状态:永久(按来源名)+ 活动(按活动实例 ID)。"""

    __slots__ = ("_permanent", "_activity")

    def __init__(self) -> None:
        self._permanent: dict[str, _Bitmap] = {}
        self._activity: dict[int, _Bitmap] = {}

    # ── 条目上限(只拦新增)─────────────────────────────────────────────
    #
    # 只在**新增**条目时校验上限:已存在的来源继续领取永不因上限被拒 ——
    # 否则调小上限或存量超限会让老玩家领不到已获得的奖励,属回档。
    # load() 进来的存量记录同理不做校验,只是从此不能再长。

    def _perm_bitmap(self, source: str) -> _Bitmap:
        existing = self._permanent.get(source)
        if existing is not None:
            return existing
        if len(source.encode("utf-8")) > MAX_SOURCE_NAME_LEN:
            raise SourceNameTooLongError()
        if len(self._permanent) >= MAX_PERMANENT_SOURCES:
            raise TooManyEntriesError()
        created = _Bitmap()
        self._permanent[source] = created
        return created

    def _act_bitmap(self, instance_id: int) -> _Bitmap:
        existing = self._activity.get(instance_id)
        if existing is not None:
            return existing
        if len(self._activity) >= MAX_ACTIVITY_INSTANCES:
            raise TooManyEntriesError()
        created = _Bitmap()
        self._activity[instance_id] = created
        return created

    # ── 永久类 ─────────────────────────────────────────────────────────

    def claim_permanent(self, source: str, index: int) -> None:
        """领取永久来源 source 的第 index 档。已领过 → AlreadyClaimedError。"""
        if index >= MAX_BIT_INDEX:
            raise IndexTooLargeError()
        bm = self._perm_bitmap(source)
        if bm.test(index):
            raise AlreadyClaimedError()
        bm.set(index)

    def is_permanent_claimed(self, source: str, index: int) -> bool:
        bm = self._permanent.get(source)
        return bm is not None and bm.test(index)

    def permanent_claimed_indices(self, source: str) -> list[int]:
        bm = self._permanent.get(source)
        return bm.set_indices() if bm is not None else []

    # ── 活动类 ─────────────────────────────────────────────────────────

    def claim_activity(self, instance_id: int, index: int) -> None:
        """领取活动实例 instance_id 的第 index 档。已领过 → AlreadyClaimedError。"""
        if index >= MAX_BIT_INDEX:
            raise IndexTooLargeError()
        bm = self._act_bitmap(instance_id)
        if bm.test(index):
            raise AlreadyClaimedError()
        bm.set(index)

    def is_activity_claimed(self, instance_id: int, index: int) -> bool:
        bm = self._activity.get(instance_id)
        return bm is not None and bm.test(index)

    def activity_claimed_indices(self, instance_id: int) -> list[int]:
        bm = self._activity.get(instance_id)
        return bm.set_indices() if bm is not None else []

    def has_activity(self, instance_id: int) -> bool:
        return instance_id in self._activity

    def erase_activity(self, instance_id: int) -> bool:
        """删除活动实例整条记录(活动下线回收)。返回是否确有该条被删除。"""
        return self._activity.pop(instance_id, None) is not None

    def retain_activities(self, active_ids: set[int]) -> int:
        """只保留仍有效的活动实例,返回清理掉的条数。空集合 = 清空全部活动记录。"""
        stale = [i for i in self._activity if i not in active_ids]
        for i in stale:
            del self._activity[i]
        return len(stale)

    def activity_ids(self) -> list[int]:
        return list(self._activity)

    # ── 序列化(落地 proto bytes)────────────────────────────────────────

    def snapshot(self) -> tuple[dict[str, bytes], dict[int, bytes]]:
        """导出落地形态。字节已去掉尾部全零;全空的条目不会出现在结果里。"""
        permanent: dict[str, bytes] = {}
        for src, bm in self._permanent.items():
            trimmed = _trimmed(bm.bits)
            if trimmed is not None:
                permanent[src] = trimmed
        activity: dict[int, bytes] = {}
        for inst, bm in self._activity.items():
            trimmed = _trimmed(bm.bits)
            if trimmed is not None:
                activity[inst] = trimmed
        return permanent, activity


def load(
    permanent: dict[str, bytes] | None, activity: dict[int, bytes] | None
) -> Record:
    """从落地形态重建 Record。入参可为 None;字节做防御性拷贝,不共享底层缓冲。"""
    rec = Record()
    for src, raw in (permanent or {}).items():
        trimmed = _trimmed(raw)
        if trimmed is not None:
            rec._permanent[src] = _Bitmap(trimmed)  # noqa: SLF001 —— 同模块内的重建入口
    for inst, raw in (activity or {}).items():
        trimmed = _trimmed(raw)
        if trimmed is not None:
            rec._activity[inst] = _Bitmap(trimmed)  # noqa: SLF001
    return rec
