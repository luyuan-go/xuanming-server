"""player_locator 用例层 —— 对应 Go 侧 internal/biz/locator.go。

★ 首要语义(§9.22),移植时最容易丢的一条:

    locator 只是 **presence / 最近活跃投影**,不是权威。

    `LOCATION_STATE_HUB` / `BATTLE` 按需查询,不在其它服务复制;
    **key miss 只能说明 presence 不可见,不能单独证明玩家已离开旧 DS,
    也不能授权进入另一台 DS。**

    这条不是文档口径而是可执行约束:任何"查不到 → 当成没人 → 放行"的写法
    都会在网络分区时放出第二个 owner。真正的归属权威是 owner 服务。

★ 两条机械保护:

1. **TTL 有机械下限**(不是调优参数)
   BATTLE presence 是 login/matchmaker 再入门的第一道信号,其 TTL 必须
   ≥ DS 授权租约上限 + 偏差余量(27s)。保证 presence 蒸发、各门放行时,
   分区的旧 DS 已对存量玩家完成自我 fencing。**配置调低会被机械抬回。**

2. **HUB presence 代际闸**
   这是"玩家秒重连后被旧连接的迟到写顶回去"的**唯一拦截点**。
   同 assignment 内 seq 回退 / 同序 ABA / 已离开的 admission 重放 /
   fenced 当前代下的 legacy 降级写,全部在这里拒。
"""

from __future__ import annotations

import dataclasses

from pandora.locator.v1 import locator_pb2

from pandorapy import errcode, placement
from pandorapy import log as plog

# 位置状态**直接引用 proto 生成物**,不手抄数值。
#
# ★ 为什么不手抄:同一套编码在 Go 常量、Lua 脚本、UE 三处各存一份,手抄错位后
#   "真 HUB 被当成另一种状态校验"不会抛错、只会静默走错分支;而单测若抄了同一个
#   错值,这条永远不会红。引用生成物让 proto 是唯一事实源,改 proto 直接传导过来。
_S = locator_pb2.LocationState
LOCATION_STATE_UNSPECIFIED = _S.LOCATION_STATE_UNSPECIFIED
LOCATION_STATE_OFFLINE = _S.LOCATION_STATE_OFFLINE
LOCATION_STATE_LOGIN_PENDING = _S.LOCATION_STATE_LOGIN_PENDING
LOCATION_STATE_HUB = _S.LOCATION_STATE_HUB
LOCATION_STATE_MATCHING = _S.LOCATION_STATE_MATCHING
LOCATION_STATE_BATTLE = _S.LOCATION_STATE_BATTLE

# 合法 state 是**闭区间**而非白名单(对齐 Go SetLocation 的 0..5 范围校验)。
# ★ 白名单写法会把 OFFLINE / LOGIN_PENDING 这两个合法枚举判成越界 —— 它们没有
#   附加必填项,所以 Go 只在 switch 里对 HUB/MATCHING/BATTLE 追加字段校验,
#   范围本身不排除它们。
MIN_LOCATION_STATE = LOCATION_STATE_UNSPECIFIED
MAX_LOCATION_STATE = LOCATION_STATE_BATTLE

# 默认 TTL。实际生效值会被 DS_FENCE_REENTRY_BARRIER 机械抬高。
DEFAULT_TTL_SEC = 30

# 拒绝 reason 枚举(§11.3 R2:一个 if 收敛 N 个条件的必须拆成 N 个 reason)。
REASON_PLAYER_ID_ZERO = "player_id_zero"
REASON_STATE_OUT_OF_RANGE = "state_out_of_range"
REASON_HUB_POD_MISSING = "hub_pod_missing"
REASON_HUB_FENCE_INCOMPLETE = "hub_fence_incomplete"
REASON_HUB_FENCE_ON_NON_HUB = "hub_fence_on_non_hub_state"
REASON_MATCH_ID_MISSING = "match_id_missing"
REASON_BATTLE_TARGET_MISSING = "battle_match_or_pod_missing"
REASON_PRESENCE_STALE = "stale_hub_presence"


@dataclasses.dataclass(frozen=True, slots=True)
class HubPresenceFence:
    """HUB presence 的代际身份。三项要么全空(legacy 模式)要么全齐(fenced 模式)。"""

    assignment_id: str = ""
    admission_id: str = ""
    admission_seq: int = 0

    def is_zero(self) -> bool:
        return not self.assignment_id and not self.admission_id and self.admission_seq == 0

    def is_complete(self) -> bool:
        return bool(self.assignment_id) and bool(self.admission_id) and self.admission_seq > 0


@dataclasses.dataclass(slots=True)
class LocationInput:
    player_id: int = 0
    state: int = 0
    hub_pod: str = ""
    shard_id: int = 0
    match_id: int = 0
    battle_pod: str = ""
    hub_presence_fence: HubPresenceFence = dataclasses.field(
        default_factory=HubPresenceFence
    )


def effective_ttl_sec(configured_sec: int) -> int:
    """把配置 TTL 钳到机械下限。

    ★ 这是**正确性下限,不是调优**:BATTLE presence 的 TTL 必须 ≥ 再入屏障(27s),
    否则 presence 先蒸发、再入门放行,而分区的旧 DS 还没完成自我 fencing →
    一名玩家同时存在于两台 DS。

    配置写小了直接抬回来,并留 WARN —— 静默抬高会让运维以为配置生效了。
    """
    ttl = configured_sec if configured_sec > 0 else DEFAULT_TTL_SEC
    floor = placement.DS_FENCE_REENTRY_BARRIER_SECONDS
    if ttl < floor:
        plog.get().warning(
            "locator_ttl_clamped_to_fence_barrier",
            configured_sec=configured_sec,
            effective_sec=floor,
            hint="presence TTL 必须 ≥ DS 再入屏障,否则旧 DS 未 fencing 完就放行新 DS",
        )
        return floor
    return ttl


def validate_location_input(inp: LocationInput) -> None:
    """SetLocation 入参校验。任一项不合法 → 拒绝并留 reason。

    ★ 每个拒绝分支一个**独立** reason:合并成"参数非法"会让线上只看到
    "这玩家位置一直写不进去",不知道缺的是哪一项。
    """
    if inp.player_id == 0:
        _reject(REASON_PLAYER_ID_ZERO, inp)

    if inp.state < MIN_LOCATION_STATE or inp.state > MAX_LOCATION_STATE:
        _reject(
            REASON_STATE_OUT_OF_RANGE,
            inp,
            state=inp.state,
            min_state=MIN_LOCATION_STATE,
            max_state=MAX_LOCATION_STATE,
        )

    # ★ 分支顺序**必须与 Go 的 switch 一致**(先按 state 分支查各自必填项,
    #   最后才统一查「非 HUB 不得带 fence」)。顺序换了不会报错,但同一个非法请求
    #   会得到**不同的 reason**:例如 state=MATCHING、match_id=0 且误带了 fence 时,
    #   Go 报 match_id_missing 而顺序颠倒的实现报 hub_fence_on_non_hub_state ——
    #   线上按 reason 建的看板和排障手册当场对不上。
    if inp.state == LOCATION_STATE_HUB:
        if not inp.hub_pod:
            _reject(REASON_HUB_POD_MISSING, inp)
        # fence 要么全空(legacy)要么全齐(fenced);**半齐是配置/代码错误**,
        # 放行会让代际闸拿着残缺身份去比,结论不可靠。
        fence = inp.hub_presence_fence
        if not fence.is_zero() and not fence.is_complete():
            _reject(
                REASON_HUB_FENCE_INCOMPLETE,
                inp,
                assignment_id=fence.assignment_id,
                admission_id=fence.admission_id,
                admission_seq=fence.admission_seq,
            )
    elif inp.state == LOCATION_STATE_MATCHING:
        if inp.match_id == 0:
            _reject(REASON_MATCH_ID_MISSING, inp)
    elif inp.state == LOCATION_STATE_BATTLE:
        if inp.match_id == 0 or not inp.battle_pod:
            _reject(
                REASON_BATTLE_TARGET_MISSING,
                inp,
                match_id=inp.match_id,
                battle_pod=inp.battle_pod,
            )

    # ★ 非 HUB 状态**不允许**带 hub fence —— 带了说明调用方状态机错乱,
    # 放行会让 BATTLE 写把 HUB 的代际信息一起刷进去,之后 HUB 的代际闸
    # 拿到的是一个从没发生过的"当前代"。
    if inp.state != LOCATION_STATE_HUB and not inp.hub_presence_fence.is_zero():
        _reject(REASON_HUB_FENCE_ON_NON_HUB, inp, state=inp.state)


def compare_uint_decimal(left: str, right: str) -> int:
    """按**十进制字符串**比较两个无符号整数。对应 Lua 侧的 compare_uint_decimal。

    ★ 为什么 Lua 侧需要它:Redis 的 Lua 数是 double,`admission_seq` 是 uint64,
    超过 2^53 就丢精度 —— 两个不同的 seq 会被判成相等,代际闸失效。

    Python 的 int 是任意精度,原生比较就是对的;这个函数存在只是为了
    **和 Lua 侧的实现对拍**(见 tests),保证两边在大数上结论一致。
    """
    lhs = left.lstrip("0") or "0"
    rhs = right.lstrip("0") or "0"
    if len(lhs) != len(rhs):
        return -1 if len(lhs) < len(rhs) else 1
    if lhs == rhs:
        return 0
    return -1 if lhs < rhs else 1


def presence_miss_is_not_departure() -> str:
    """把 §9.22 那条语义写成可被引用的常量说明(供调用方注释指向)。

    key miss ⇒ presence 不可见,**不** ⇒ 玩家已离开旧 DS。
    需要"玩家现在归谁"必须查 owner 权威。
    """
    return (
        "locator key miss 只说明 presence 不可见;它不能证明玩家已离开旧 DS,"
        "也不能授权进入另一台 DS。归属判定必须查 owner 权威(§9.22)。"
    )


def _reject(reason: str, inp: LocationInput, **extra) -> None:
    plog.get().warning(
        "locator_set_rejected",
        reason=reason,
        player_id=inp.player_id,
        hub_pod=inp.hub_pod,
        **extra,
    )
    raise errcode.PandoraError(errcode.ErrInvalidArg, "locator set rejected: %s", reason)


def stale_presence_error(player_id: int, fence: HubPresenceFence) -> errcode.PandoraError:
    """代际闸拒绝 —— 本次 HUB 写来自旧代连接。

    这是"玩家秒重连后被旧连接迟到写顶回去"的唯一拦截点,必须留证:
    期望值(当前代)在 Lua 内比对拿不到,所以并排打出本次 incoming 的全量身份,
    供与 hub_allocator 侧对账。
    """
    plog.get().warning(
        "locator_set_rejected",
        reason=REASON_PRESENCE_STALE,
        player_id=player_id,
        assignment_id=fence.assignment_id,
        admission_id=fence.admission_id,
        admission_seq=fence.admission_seq,
    )
    return errcode.PandoraError(
        errcode.ErrLocatorConflict,
        "player %d reject stale HUB presence assignment=%s admission_seq=%d",
        player_id,
        fence.assignment_id,
        fence.admission_seq,
    )
