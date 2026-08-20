"""DS 实例身份 + fence 租约协议常量 —— 对应 Go 侧 pkg/placement。

★ 这些是**正确性常量,不是调优参数**(CLAUDE.md §9.22):
    调大只增加故障恢复延迟;调小会重新打开「一名玩家同时存在于两台 DS」的脑裂窗口。
    跨仓契约 —— UE 侧 UPandoraDSBackendSubsystem 有一份对应实现,两边必须同值。

协议(docs/design/battle-reconnect.md §8):

  1. DS 以最近一次「绑定 active 凭据的权威心跳响应」为租约起点(**单调时钟**)。
     连续 DS_FENCE_LEASE_MAX_SECONDS 未能续租 → DS 必须对**存量玩家**自我 fencing:
     关闭输入、Kick 所有已准入连接、销毁 Pawn(**不只是拒新玩家**)。

  2. 服务端任何「把静默 DS 上的玩家交给新 DS」的再入门,必须等待该 DS 的
     last_heartbeat_ms 至少经过 DS_FENCE_REENTRY_BARRIER。
     由此保证核心时序:**旧 DS 最晚停止可玩时间 < 新 DS 最早开始可玩时间**。

  3. player_locator TTL 与 hub_allocator heartbeat_timeout 都必须 ≥ 再入屏障
     (当前默认 30s ≥ 27s,启动时有机械下限保护)。
"""

from __future__ import annotations

import dataclasses
import re
import uuid as _uuid

_UINT32_MAX = (1 << 32) - 1

# DS 侧授权租约的协议上限(秒)。UE 侧把租约硬钳在 [5, 本值],配置无法放大。
DS_FENCE_LEASE_MAX_SECONDS = 20

# 安全余量(秒)。预算构成必须完整覆盖三项,不能按单机观测的平均延迟缩小:
#   ① 心跳响应在途上限(UE HeartbeatRequestTimeoutSeconds = 4s)
#   ② fencing 检测粒度(1s ticker)
#   ③ 服务间时钟漂移专属预留(≥2s)—— ds_allocator 写 last_heartbeat_ms 与
#      login 读 now() 是**两台机器的时钟**
# 2026-07-18 从 5 提到 7:原值被前两项恰好占满,时钟漂移零预留。
DS_FENCE_SKEW_MARGIN_SECONDS = 7

# 服务端再入屏障:自 DS 最后一次心跳起必须经过该时长,才允许把这台 DS 上的玩家
# 路由到任何新 DS。派生值 = 27s,保持"租约上限 + 余量"只有一个权威计算入口。
DS_FENCE_REENTRY_BARRIER_SECONDS = DS_FENCE_LEASE_MAX_SECONDS + DS_FENCE_SKEW_MARGIN_SECONDS

# canonical 小写 RFC4122 UUID 的形状。先用正则挡掉大写 / 花括号 / URN 前缀这些
# uuid.UUID() 会**接受但改写**的形式 —— 我们要的是"原样 canonical",不是"能解析"。
# ⚠️ 用 `\A`/`\Z` 而不是 `^`/`$`:Python 的 `$` 匹配"字符串末尾**或末尾换行之前**",
# 于是末尾带一个换行的串会被正则放过。这里目前还有下面 `str(parsed) == value`
# 那道二次校验兜着,但把洞留在正则里等于赌将来没人删那道校验。
# (2026-08-18 在 auction 的幂等键正则上真的踩到了这个陷阱,顺手全仓加固。)
_CANONICAL_UUID_RE = re.compile(
    r"\A[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\Z"
)


def valid_operation_id(value: str) -> bool:
    """只接受 canonical 小写 RFC4122 UUIDv4。对应 Go 的 ValidOperationID。

    ★ 为什么要求"canonical 原样相等"而不只是"能解析":
        operation_id 是 §9.23 的端到端幂等键。`uuid.UUID(s)` 会接受大写、
        带花括号 `{...}`、带 `urn:uuid:` 前缀等多种写法并归一化 —— 如果只校验
        "能解析",同一次进场用不同写法重试就会被当成**两个不同的 operation**,
        幂等键失效,于是重复占座 / 重复分配 DS / 产生第二个 owner。
        Go 侧靠 `id.String() == value` 挡这一层,这里靠正则 + 版本/变体复核。
    """
    if not isinstance(value, str) or not _CANONICAL_UUID_RE.match(value):
        return False
    try:
        parsed = _uuid.UUID(value)
    except ValueError:
        return False
    return (
        parsed.version == 4
        and parsed.variant == _uuid.RFC_4122
        and int(parsed) != 0
        and str(parsed) == value
    )


def new_operation_id() -> str:
    """铸一个新的 canonical operation_id。"""
    return str(_uuid.uuid4())


# ── Go unicode 语义的精确复刻 ────────────────────────────────────────────────
#
# 为什么不能直接用 Python 的 str.strip() / str.isspace():两边的"空白"词表不同。
#   - Python 的 str.isspace() 认 \x1c-\x1f(文件/组/记录/单元分隔符)是空白;
#     Go 的 unicode.IsSpace 按 Unicode White_Space 属性,**不认**这四个。
#   - Go 的 unicode.IsControl 只覆盖 Latin-1 区(U+0000-U+001F、U+007F-U+009F),
#     Latin-1 之外一律 false。用 unicodedata.category(c).startswith("C") 会比 Go 严
#     (把 U+200E 这类 Cf 格式字符也算进去)。
# 这些差异最终落在 battleabort 的"签名前字段校验"上:比 Go 严会让 Python 副本拒掉
# Go 能签的合法 abort(双栈并行期表现为偶发的 abort 失败),比 Go 松会放进 Go 拒绝的
# 字节。两个方向都不可接受,所以照抄 Go 的词表而不是借 Python 的。
_GO_WHITE_SPACE = frozenset(
    "\t\n\v\f\r \x85\xa0"
    "\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a"
    "\u2028\u2029\u202f\u205f\u3000"
)


def go_is_space(ch: str) -> bool:
    """对应 Go 的 unicode.IsSpace。"""
    return ch in _GO_WHITE_SPACE


def go_is_control(ch: str) -> bool:
    """对应 Go 的 unicode.IsControl —— 只有 Latin-1 区的 C0/C1 算控制字符。"""
    cp = ord(ch)
    return cp <= 0x1F or 0x7F <= cp <= 0x9F


def go_trim_space(value: str) -> str:
    """对应 Go 的 strings.TrimSpace(按 go_is_space 的词表裁剪两端)。"""
    start, end = 0, len(value)
    while start < end and go_is_space(value[start]):
        start += 1
    while end > start and go_is_space(value[end - 1]):
        end -= 1
    return value[start:end]


# ── DS 实例身份元组(对应 Go 的 placement.Target)──────────────────────────────


@dataclasses.dataclass(frozen=True)
class Target:
    """精确 DS 实例身份(Hub assignment 或 Battle allocation)。

    ★ 为什么 pod_name 一个字段不够:
        k8s 会用**同名** Pod 重建实例。只按 pod_name 比较的话,新旧两个实例长得一模一样,
        旧实例的迟到写就能冒充当前 owner(§9.22 的 exact 实例绑定要防的正是这个)。
        instance_uid + instance_epoch 让同名替换永远比不相等。
    """

    pod_name: str = ""
    instance_uid: str = ""
    instance_epoch: int = 0
    assignment_id: str = ""
    allocation_id: str = ""
    release_track: str = ""

    def complete_hub(self) -> bool:
        """Hub 侧五要件齐备(带 assignment_id)。对应 Go 的 Target.CompleteHub。"""
        return (
            go_trim_space(self.pod_name) != ""
            and go_trim_space(self.instance_uid) != ""
            and self._epoch_positive()
            and go_trim_space(self.assignment_id) != ""
            and go_trim_space(self.release_track) != ""
        )

    def complete_battle(self) -> bool:
        """Battle 侧五要件齐备(带 allocation_id)。对应 Go 的 Target.CompleteBattle。"""
        return (
            go_trim_space(self.pod_name) != ""
            and go_trim_space(self.instance_uid) != ""
            and self._epoch_positive()
            and go_trim_space(self.allocation_id) != ""
            and go_trim_space(self.release_track) != ""
        )

    def equal(self, other: Target) -> bool:
        """六个字段全等。对应 Go 的 Target.Equal。"""
        return (
            self.pod_name == other.pod_name
            and self.instance_uid == other.instance_uid
            and self.instance_epoch == other.instance_epoch
            and self.assignment_id == other.assignment_id
            and self.allocation_id == other.allocation_id
            and self.release_track == other.release_track
        )

    def _epoch_positive(self) -> bool:
        """instance_epoch 在 Go 里是 uint32,`> 0` 就够。

        Python 的 int 是无限精度,`> 0` 放得进 2**40 这种 Go 根本存不下的值 ——
        一个 Go 侧不可能出现的 epoch 通过了 Python 的 complete 校验,就会被签进
        abort body / 写进 Redis,而 Go 副本读回来时 uint32 装不下直接解码失败。
        所以这里必须把 Go 的静态类型显式补成运行时上界检查。
        """
        return (
            isinstance(self.instance_epoch, int)
            and 0 < self.instance_epoch <= _UINT32_MAX
        )

