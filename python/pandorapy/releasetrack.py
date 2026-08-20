"""stable / canary 的确定性 cohort 选择 —— 对应 Go 侧 pkg/releasetrack/policy.go。

★ 这个模块为什么必须逐位对齐 Go(唯一理由):
    §9.21 要求"同一玩家 / 同一对局固定 release track"。双栈并行期,同一个 player_id
    可能这次被 Go 版 hub_allocator 处理、下次被 Python 版处理。两边算出不同 track 时:
      - 不报错
      - 玩家在 stable 与 canary 两个 Fleet 之间来回漂
      - 灰度比例失真,回滚时"把 Canary 权重归零"也拦不住已经粘到 canary 的人
    所以选择函数必须是**跨语言逐位一致的纯函数**:同样的 seed + player_id,
    两个实现的 sha256 前 8 字节大端取模结果必须一模一样。
    tests/test_releasetrack.py 用 `go run` 对 220 个样本逐个对拍,不允许 1 例不一致。

★ 选择结果只是**分配意图**,不是事实(照抄 Go 头注释的告诫):
    canary 无容量时会回退 stable(见 hub.go 的 ErrHubNoAvailable 分支)。调用方必须
    持久化编排层权威回读到的**实际** track,不能把 Policy.select() 的返回值当最终事实。
"""

from __future__ import annotations

import dataclasses
import hashlib

# 轨道名是**跨语言 / 跨仓库的裸字符串常量**,不是 proto 枚举:
# proto 里 release_track 全部声明为 `string`(locator / login / hub / match 各 pb 均如此),
# Agones Fleet 名、Redis 里已落盘的 HubAssignment.release_track 也都是这两个字面量。
# 所以这里只能是字符串,不能改成 IntEnum —— 改了会让已有记录一个都读不出来。
STABLE = "stable"
CANARY = "canary"

_PERCENT_MAX = 100
_UINT64_MAX = (1 << 64) - 1


def valid(track: str) -> bool:
    """对应 Go 的 releasetrack.Valid —— 只认这两个字面量。

    注意这是 fail-closed:空串 / 未知轨道一律 false。Go 侧 hub_allocator 对
    "pre-track 的历史记录"是在**调用方**补 Stable 默认值(hub_authoritative.go:465),
    不是在这里放行;本函数不能替调用方做那个兜底,否则任何脏数据都会被当 stable。
    """
    return track == STABLE or track == CANARY


@dataclasses.dataclass(frozen=True)
class Policy:
    """不可变的灰度策略。用 new() 构造,不要直接实例化(会绕过范围校验)。"""

    percent: int
    seed: str

    def select(self, player_id: int) -> str:
        """确定性地把一个 ID 分到 stable 或 canary。对应 Go 的 Policy.Select。

        四条判据的**顺序**必须照抄,不能重排:
          1. percent == 0 或 id == 0 → stable
          2. percent == 100 → canary
          3. sha256(seed + ":" + 十进制 id) 前 8 字节大端 % 100 < percent → canary
          4. 否则 stable
        第 1 步在第 2 步之前意味着 **percent=100 且 id=0 时返回 stable**。看起来像笔误,
        实际是 Go 的真实行为(policy.go 两个 if 的先后),而 id==0 代表"没有玩家上下文"
        的分配,让它落 stable 是保守方向。把 2 提到 1 前面会让这类分配全跑去 canary。
        """
        if not isinstance(player_id, int) or player_id < 0 or player_id > _UINT64_MAX:
            # Go 的签名是 Select(id uint64),负数 / 超 64 位在那边根本编译不出来。
            # Python 的 int 无限精度,不显式挡就会静默算出一个 Go 永远得不到的 track。
            raise ValueError(f"player_id {player_id!r} out of uint64 range")
        if self.percent == 0 or player_id == 0:
            return STABLE
        if self.percent == _PERCENT_MAX:
            return CANARY
        digest = hashlib.sha256(f"{self.seed}:{player_id}".encode("utf-8")).digest()
        # Go: binary.BigEndian.Uint64(sum[:8]) % 100。取前 8 字节、大端、无符号。
        # 用 int.from_bytes 而不是 struct:两者等价,但这里想让"只取 8 字节"显式可见。
        bucket = int.from_bytes(digest[:8], "big") % _PERCENT_MAX
        return CANARY if bucket < self.percent else STABLE


def new(percent: int, seed: str) -> Policy:
    """构造策略。对应 Go 的 releasetrack.New(percent uint32, seed string)。

    两条 fail-closed 前置(方向与 Go 完全一致,一处都不能反):
      - percent > 100 拒绝。Go 用 uint32 挡住负数,Python 必须显式挡 —— 否则
        percent=-1 会让第 3 步的 `bucket < self.percent` 恒 false,灰度**静默全关**,
        运维看配置以为在放量,实际一个人都没进 canary。
      - percent > 0 而 seed 为空拒绝。空 seed 不是"随机",而是让全服的分桶只由
        player_id 决定:两次发布会选中**完全相同**的一批玩家反复当小白鼠,
        且任何人都能离线算出自己是否在 canary。
    """
    if not isinstance(percent, int) or percent < 0 or percent > _PERCENT_MAX:
        raise ValueError(f"canary_percent {percent} out of range [0,100]")
    if percent > 0 and seed == "":
        raise ValueError("canary_seed required when canary_percent > 0")
    return Policy(percent=percent, seed=seed)
