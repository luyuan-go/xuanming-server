"""fence / lease 时序常量与不等式 —— 跨 pkg/placement、pkg/dsauthfence/writerlease、
两个 allocator 与 player_locator 的**唯一校验入口**。

★ 为什么单独立这个模块:

    这些常量散落在五个地方(placement / writerlease / hub_allocator conf /
    ds_allocator conf / locator conf),彼此之间有**必须成立的不等式**。
    单看任何一个数字都合理,但组合起来可能已经把脑裂窗口打开了 ——
    而这种破坏**没有任何运行期信号**:服务照常启动、心跳照常上报,
    只在某次网络分区时表现为"一个玩家同时在两台 DS 上"。

    Go 侧的做法是在各服务的 conf 里各写一句 `if x < barrier { x = barrier }`
    (机械抬回)。那是对的,但校验逻辑本身也散着 —— 没有一处能回答
    "现在这套数字整体自洽吗"。本模块把整条时间线收成可断言的形式。

★ 核心不等式(§9.22):

        旧 DS 最晚停止可玩时间  <  新 DS 最早开始可玩时间

    它由下面几条支撑,任何一条破了这个不等式就不成立。
"""

from __future__ import annotations

import dataclasses

from pandorapy import placement, writerlease

# ── writerlease(单写者选举)────────────────────────────────────────────────
#
# ★ 这里**引用**而不是抄写 writerlease 的常量。
#
# 本模块自称是这条时间线的「唯一校验入口」,而抄一份副本会让这句话变成假的:
# 真常量被改坏时,check_timeline() 校验的是自己手里那份没被改的副本 ——
# **全绿**。防护型模块自己先失去牙齿,是最坏的一种失效。
#
# 每个别名下面的注释解释「这个数为什么是这个数」,取值本身以 writerlease 为准。

# 崩溃场景的最大接任延迟 ≈ 此值;正常滚动更新走主动 Resign,亚秒接任。
WRITER_LEASE_TTL_SEC = writerlease.DEFAULT_LEASE_TTL_SEC

# 「本地安全截止时间」相对 etcd lease TTL 的提前量。
#
# ★ 必须**早于**服务端 lease 真正过期:etcd 侧续租是周期性的,本地不可能精确知道
# 服务端何时判定过期;宁可自己先停手,也不要在服务端已经把任期交给别人之后
# 还认为自己持有。3s 覆盖一次续租往返 + 时钟抖动。
WRITER_HOLD_SAFETY_MARGIN_SEC = writerlease.HOLD_SAFETY_MARGIN_SEC

# 激活钩子(OnElected)的独立总期限。
#
# ★ 必须有界的理由:激活期间本副本**已当选并持有 etcd leader key**,却还没对外
# 宣告持有。钩子若永久阻塞:① 本副本永远不可写;② 它占着 leader key 不让位,
# 其它副本全部排队 —— **整个集群进入无写者状态**,而失败计数一次都不会 +1
# (计数只在 err != nil 分支),长期无主完全静默。
WRITER_ACTIVATION_TIMEOUT_SEC = writerlease.DEFAULT_ACTIVATION_TIMEOUT_SEC

# 失主 / 出错后重新竞选的退避。
WRITER_RECAMPAIGN_BACKOFF_SEC = writerlease.RECAMPAIGN_BACKOFF_SEC

# 连续竞选失败达此次数后日志从 WARN 升 ERROR(无限重试不能 fail-silent)。
WRITER_CAMPAIGN_ESCALATE_AFTER = writerlease.CAMPAIGN_ESCALATE_AFTER

# ── 各服务的心跳超时(不变量 §9.4)──────────────────────────────────────────

# Battle DS:15s 超时 → abandoned → 段位回滚。
BATTLE_HEARTBEAT_TIMEOUT_SEC = 15
# Hub DS:30s 超时 → draining / 停止分配。
HUB_HEARTBEAT_TIMEOUT_SEC = 30
# player_locator 的 presence TTL。
LOCATOR_TTL_SEC = 30


@dataclasses.dataclass(frozen=True, slots=True)
class Violation:
    name: str
    detail: str


def check_timeline() -> list[Violation]:
    """校验整条 fence / lease 时间线。返回违规列表(空 = 全部自洽)。

    每一条都写清"破了会怎样",因为这些不等式的共同点是:
    **破了不会报错,只在故障时表现为脑裂或长期无主**。
    """
    v: list[Violation] = []

    # ① 再入屏障 = 租约上限 + 偏差余量。这是派生值,必须保持单一计算入口。
    expected_barrier = (
        placement.DS_FENCE_LEASE_MAX_SECONDS + placement.DS_FENCE_SKEW_MARGIN_SECONDS
    )
    if placement.DS_FENCE_REENTRY_BARRIER_SECONDS != expected_barrier:
        v.append(
            Violation(
                "reentry_barrier_derivation",
                f"屏障 {placement.DS_FENCE_REENTRY_BARRIER_SECONDS} != "
                f"租约上限 {placement.DS_FENCE_LEASE_MAX_SECONDS} + "
                f"余量 {placement.DS_FENCE_SKEW_MARGIN_SECONDS} —— "
                f"两处各写一个数会漂移",
            )
        )

    # ② 偏差余量必须覆盖三项预算:心跳在途(4s)+ fencing 检测粒度(1s)+ 时钟漂移(≥2s)。
    if placement.DS_FENCE_SKEW_MARGIN_SECONDS < 4 + 1 + 2:
        v.append(
            Violation(
                "skew_margin_budget",
                f"余量 {placement.DS_FENCE_SKEW_MARGIN_SECONDS}s 不足 7s —— "
                f"心跳在途 4s + 检测粒度 1s + 时钟漂移 ≥2s。"
                f"时钟漂移零预留时,两台机器的时钟差就能打开脑裂窗口",
            )
        )

    # ③ locator TTL 与 Hub 心跳超时都必须 ≥ 再入屏障。
    #
    # 破了会怎样:presence 先蒸发 / Hub 先判超时 → 再入门放行 →
    # 而分区的旧 DS 还没完成自我 fencing → 一个玩家同时在两台 DS。
    for name, value in (
        ("locator_ttl", LOCATOR_TTL_SEC),
        ("hub_heartbeat_timeout", HUB_HEARTBEAT_TIMEOUT_SEC),
    ):
        if value < placement.DS_FENCE_REENTRY_BARRIER_SECONDS:
            v.append(
                Violation(
                    name,
                    f"{name}={value}s < 再入屏障 "
                    f"{placement.DS_FENCE_REENTRY_BARRIER_SECONDS}s —— "
                    f"presence 先蒸发而旧 DS 未 fencing 完,脑裂窗口打开",
                )
            )

    # ④ writer lease 的本地安全窗必须为正,且余量要真正小于 TTL。
    local_window = WRITER_LEASE_TTL_SEC - WRITER_HOLD_SAFETY_MARGIN_SEC
    if local_window <= 0:
        v.append(
            Violation(
                "writer_local_window",
                f"本地安全窗 {local_window}s <= 0(TTL {WRITER_LEASE_TTL_SEC} - "
                f"余量 {WRITER_HOLD_SAFETY_MARGIN_SEC})—— 副本永远认为自己已失主",
            )
        )
    if WRITER_HOLD_SAFETY_MARGIN_SEC <= 0:
        v.append(
            Violation(
                "writer_safety_margin",
                "安全余量 <= 0 —— 本地会在服务端已把任期交给别人之后仍认为自己持有",
            )
        )

    # ⑤ 激活超时必须 > lease TTL。
    #
    # 破了会怎样:激活还没超时,lease 已经过期 —— 本副本占着 key 却既不可写
    # 也不让位,而计数器不动(只在 err 分支加),**长期无主完全静默**。
    if WRITER_ACTIVATION_TIMEOUT_SEC <= WRITER_LEASE_TTL_SEC:
        v.append(
            Violation(
                "activation_timeout",
                f"激活超时 {WRITER_ACTIVATION_TIMEOUT_SEC}s <= lease TTL "
                f"{WRITER_LEASE_TTL_SEC}s —— 无主状态会在计数器动起来之前就发生",
            )
        )

    # ⑥ 告警阈值对应的时长必须 > lease TTL(否则正常的一次接任就会触发告警)。
    escalate_sec = WRITER_CAMPAIGN_ESCALATE_AFTER * WRITER_RECAMPAIGN_BACKOFF_SEC
    if escalate_sec <= WRITER_LEASE_TTL_SEC:
        v.append(
            Violation(
                "escalate_threshold",
                f"告警阈值 {escalate_sec}s <= lease TTL {WRITER_LEASE_TTL_SEC}s —— "
                f"一次正常的崩溃接任就会告警,告警会被当噪音忽略",
            )
        )

    # ⑦ Battle 心跳超时 < Hub 心跳超时。
    #
    # 这不是随意的:对局 DS 挂了要**尽快**判弃并回滚段位(玩家在等结果);
    # Hub DS 挂了只是停止分配,存量玩家还能玩一会儿,可以宽松些。
    if BATTLE_HEARTBEAT_TIMEOUT_SEC >= HUB_HEARTBEAT_TIMEOUT_SEC:
        v.append(
            Violation(
                "heartbeat_ordering",
                f"Battle 超时 {BATTLE_HEARTBEAT_TIMEOUT_SEC}s >= Hub 超时 "
                f"{HUB_HEARTBEAT_TIMEOUT_SEC}s —— 对局判弃比大厅还慢,玩家白等",
            )
        )

    return v


def core_inequality_holds(
    *, old_ds_stop_sec: float, new_ds_start_sec: float
) -> bool:
    """核心不等式:旧 DS 最晚停止可玩时间 < 新 DS 最早开始可玩时间(§9.22)。

    两个时刻都相对"旧 DS 最后一次成功心跳"计。
    """
    return old_ds_stop_sec < new_ds_start_sec


def old_ds_latest_stop_sec() -> float:
    """旧 DS 最晚停止可玩的时刻(相对最后一次成功心跳)。

    DS 侧连续 DS_FENCE_LEASE_MAX_SECONDS 未能续租即自我 fencing
    (关输入、Kick 已准入连接、销毁 Pawn)。
    """
    return float(placement.DS_FENCE_LEASE_MAX_SECONDS)


def new_ds_earliest_start_sec() -> float:
    """新 DS 最早开始可玩的时刻(相对旧 DS 最后一次成功心跳)。

    服务端再入门要求经过再入屏障 = 租约上限 + 偏差余量。
    """
    return float(placement.DS_FENCE_REENTRY_BARRIER_SECONDS)
