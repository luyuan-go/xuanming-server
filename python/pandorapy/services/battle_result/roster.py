"""战绩名单校验 —— 对应 Go 侧 internal/biz/battle_result.go 的
validateAuthorizedResultRoster / 计分判据。

★ 把每一项结算副作用绑到**权威名单**上(§9.6 五要件之②owner 授权):

    权威名单来自凭据校验时抓取的 canonical BattleStorageRecord,
    **不从 DS 上报的请求体里补值**。DS 只报事实,名单不是它说了算。

    比对是**集合相等**:stat 的顺序不是权威信号,所以不比顺序;
    但重复 stat、遗漏、外人**全部拒绝**,而且在 MMR / 落库之前就拒。

    整体拒绝的含义:本场结算不落库、不发段位、不发掉落。
    宁可一场战绩丢掉,也不能让一个不在名单里的玩家拿到段位或掉落。

★ 拒绝原因必须拆成**单一枚举**(§11.3 R2):
    一个 if 收敛了 8 个条件的话,线上只看到"名单不匹配",
    不知道是数量对不上、有重复、还是混进了外人 —— 这三者的处置完全不同。
"""

from __future__ import annotations

from pandora.config.v1 import level_pb2

from pandorapy import errcode

# 拒绝原因枚举。只作日志判据,不参与控制流 —— 每个 reason 对应的返回错误
# 与拆分前逐字节一致(拆分是为了可观测,不是为了改行为)。
REJECT_NIL_RESULT = "nil_result"
REJECT_AUTHORITY_EMPTY = "authority_roster_empty"
REJECT_COUNT_MISMATCH = "count_mismatch"
REJECT_AUTHORITY_ZERO_ID = "authority_zero_player_id"
REJECT_AUTHORITY_DUP = "authority_duplicate_player"
REJECT_REPORTED_ZERO_ID = "reported_zero_player_id"
REJECT_REPORTED_DUP = "reported_duplicate_player"
REJECT_OUTSIDER = "reported_outsider"

# 三种错误文案(与 Go 逐字一致 —— 它们会进 access log,运维按文案 grep)。
_MSG_MISMATCH = "battle result roster does not match authority"
_MSG_AUTHORITY_INVALID = "battle authority roster is invalid"
_MSG_REPORTED_INVALID = "battle result roster contains an invalid player"
_MSG_REPORTED_DUP = "battle result roster contains a duplicate player"
_MSG_OUTSIDER = "battle result roster contains an unauthorized player"


def validate_authorized_roster(
    reported_player_ids: list[int] | None, authoritative: list[int]
) -> tuple[str, int]:
    """校验 DS 上报的 stats 名单是否与权威名单一致。

    返回 (reason, sample_player_id) —— 两者只供调用方打日志
    (哪一项对不上、哪个 player_id 是第一现场)。
    通过时返回 ("", 0);不通过时**抛异常**。

    判定顺序与 Go 一致,顺序本身是契约:先判权威名单自身合法性,
    再判上报名单 —— 权威名单脏的话,拿它去比对上报名单没有意义。
    """
    if reported_player_ids is None:
        raise _reject(REJECT_NIL_RESULT, _MSG_MISMATCH)
    if not authoritative:
        raise _reject(REJECT_AUTHORITY_EMPTY, _MSG_MISMATCH)
    if len(reported_player_ids) != len(authoritative):
        # ★ 先比数量:遗漏和多报都会在这里被抓住,不用等到逐个查集合。
        raise _reject(REJECT_COUNT_MISMATCH, _MSG_MISMATCH)

    want: set[int] = set()
    for player_id in authoritative:
        if player_id == 0:
            raise _reject(REJECT_AUTHORITY_ZERO_ID, _MSG_AUTHORITY_INVALID)
        if player_id in want:
            # 权威名单自己有重复 = 上游出了问题,不能拿它当基准。
            raise _reject(REJECT_AUTHORITY_DUP, _MSG_AUTHORITY_INVALID, player_id)
        want.add(player_id)

    seen: set[int] = set()
    for player_id in reported_player_ids:
        if player_id == 0:
            raise _reject(REJECT_REPORTED_ZERO_ID, _MSG_REPORTED_INVALID)
        if player_id in seen:
            # 重复 stat:数量校验已经过了,说明必然同时有人被遗漏 —— 但重复本身
            # 就足以拒绝(否则同一玩家会拿两份结算)。
            raise _reject(REJECT_REPORTED_DUP, _MSG_REPORTED_DUP, player_id)
        if player_id not in want:
            # ★ 外人:不在权威名单里的玩家想拿段位 / 掉落。
            raise _reject(REJECT_OUTSIDER, _MSG_OUTSIDER, player_id)
        seen.add(player_id)

    return "", 0


class RosterRejected(errcode.PandoraError):
    """名单校验拒绝。携带 reason / sample_player_id 供调用方打日志。"""

    def __init__(self, code: int, msg: str, reason: str, sample_player_id: int) -> None:
        super().__init__(code, "%s", msg)
        self.reason = reason
        self.sample_player_id = sample_player_id


def _reject(reason: str, msg: str, sample_player_id: int = 0) -> RosterRejected:
    return RosterRejected(errcode.ErrUnauthorized, msg, reason, sample_player_id)


# ── 计分判据 ────────────────────────────────────────────────────────────────

# canonical PVE walk-in 部署的 game_mode。
#
# ⚠️ 2026-08-11 起**只作旧局兜底,不再是计分判据**。
#
# 原因值得记住:原口径把「算不算段位」挂在**撮合池标识**上,而且是**排除法**
# (`game_mode != "pve_coop"` 就按 Elo 算)。game_mode 是会不断新增取值的部署标识
# (未来的 "casual_5v5" / "custom"),于是**任何新池在旧口径下都会静默按排位改玩家段位** ——
# 而改段位不可逆,是最坏的失败方向。
#
# 现在的权威判据是关卡表 rating_mode 列定格进 canonical BattleStorageRecord
# 的 TerminalReleaseRecord.RatingMode(§17.1 差异进表)。
CANONICAL_GAME_MODE_PVE_COOP = "pve_coop"

# 计分模式取值**直接引用 proto 生成物**,不手抄字面量:
# 这三个值同时被 Go 结算、关卡表导出、Python 副本读,任何一侧抄错都表现为
# 「同一场对局两边算出不同段位」—— 而段位改动不可逆,错了没法回滚。
RATING_MODE_UNSPECIFIED = level_pb2.LEVEL_RATING_MODE_UNSPECIFIED
RATING_MODE_NONE = level_pb2.LEVEL_RATING_MODE_NONE  # 不计段位
RATING_MODE_ELO = level_pb2.LEVEL_RATING_MODE_ELO  # 计 Elo


# 判据来源标识。**必须与 Go 逐字一致**(battle_result.go:347-365):它们会进日志,
# 而"回落旧口径的局"正是靠 basis 值筛出来的 —— 字符串对不上,Loki 上按它建的查询就空了。
BASIS_LEGACY_NO_CANONICAL = "legacy_no_canonical"
BASIS_RATING_MODE_NONE = "rating_mode_none"
BASIS_RATING_MODE_ELO = "rating_mode_elo"
BASIS_LEGACY_CANONICAL_PVE_COOP = "legacy_canonical_pve_coop"
BASIS_LEGACY_CANONICAL_GAME_MODE = "legacy_canonical_game_mode"

# 需要打 WARN 的两个 basis:本局的 canonical rating_mode 没定格,是按旧口径算的。
LEGACY_FALLBACK_BASES = frozenset(
    {BASIS_LEGACY_CANONICAL_PVE_COOP, BASIS_LEGACY_CANONICAL_GAME_MODE}
)


def settlement_runs_elo(
    rating_mode: int | None, legacy_game_mode: str = ""
) -> tuple[bool, str]:
    """判断本场是否计段位,**并给出判据来源**。对应 Go settlementRunsElo(battle_result.go:347)。

    `rating_mode is None` 表示**连 canonical 快照都没有**(legacy kafka / 内部直调),
    对应 Go 的 `terminalRelease == nil` 分支 —— 与"有快照但 rating_mode 未定格"是
    两件不同的事,basis 也不同,不能合并成一个 0。

    ★ 为什么要把 basis 一起返回:Go 的调用方对回落旧口径的局打
    `battle_rating_basis_legacy_fallback`(WARN,带 match_id/map_id/game_mode/
    rating_pool/run_elo/basis/hint)。只返 bool 的话,**旧口径兜底的局会静默结算** ——
    段位改动不可逆,事后想追"这一局到底按什么算的"就没有任何痕迹了。
    """
    if rating_mode is None:
        # legacy kafka / 内部直调:本就没有 canonical 快照,保持历史行为照算 Elo。
        return True, BASIS_LEGACY_NO_CANONICAL
    if rating_mode == RATING_MODE_ELO:
        return True, BASIS_RATING_MODE_ELO
    if rating_mode == RATING_MODE_NONE:
        return False, BASIS_RATING_MODE_NONE
    if legacy_game_mode == CANONICAL_GAME_MODE_PVE_COOP:
        return False, BASIS_LEGACY_CANONICAL_PVE_COOP
    return True, BASIS_LEGACY_CANONICAL_GAME_MODE


def should_apply_rating(rating_mode: int, legacy_game_mode: str = "") -> bool:
    """只要结论、不要判据时的便捷入口。

    ⚠️ **接线真正的结算路径时必须用 `settlement_runs_elo` 并把 basis 打进日志**,
    否则就丢掉了 Go 刻意留的那条审计线(见该函数注释)。这个薄封装只供
    单纯判定的场合(如测试、只读校验)。

    判断本场是否计段位。对应 Go settlementRunsElo(battle_result.go:347)。

    判定优先级(与 Go 逐分支一致):
      ① canonical rating_mode 显式 NONE → 不计;显式 ELO → 计。
      ② rating_mode 未定格(滚动升级期的旧 matchmaker / 旧批次表,§9.21)→
         回落**旧口径**:game_mode == pve_coop 才不计,其余一律计。
      ③ 连 canonical 快照都没有(legacy kafka / 内部直调,调用方传 0 + "")→ 计。

    ★ ②③ 两条为什么是「计」而不是「不计」:这两条路径上的对局在本列上线前
    **本来就在算 Elo**。缺字段就跳过计分,受害的是正在打排位的玩家 —— 一整局白打
    且无从追认;而这段兜底代码随旧局一起退役,不会长期扩大影响面。
    真正要防的「新池静默按排位改段位」由分支①拦截:新关卡只要把 rating_mode
    定格成 NONE 就绝不计分,不必依赖池名。
    """
    return settlement_runs_elo(rating_mode, legacy_game_mode)[0]
