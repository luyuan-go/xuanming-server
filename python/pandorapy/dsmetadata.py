"""投递给 Dedicated Server 的权威 metadata 规范化 —— 对应 Go 侧 pkg/dsmetadata/roster.go。

DS 拿到的 roster / 阵营是通过 Agones GameServer annotation(纯文本)传的,所以这里
产出的是"升序去重的 ID 列表 + 一个逗号分隔的 annotation 串"。两件事必须同时成立:

  1. **同一批玩家必须产出同一个串**。annotation 会参与 DS 侧的对账与日志关联;
     顺序不定的话,同一局在 Go 副本与 Python 副本上会写出两个不同的 annotation,
     排障时按串比对直接对不上。所以排序是**协议的一部分**,不是"顺手排一下"。
  2. **越界一律拒绝分配,绝不截断**。Go 头注释写死了"调用方不得截断 roster 后继续分配" ——
     截断的后果是被砍掉的玩家永远进不了这局 DS,而分配本身"成功"了,
     客户端只会看到无限 loading(§9.20)。
"""

from __future__ import annotations

MAX_BATTLE_ROSTER_PLAYERS = 128

# ★ 注意 Go 源码写的是 `1<<31 - 3`,在 Go 里移位优先级**高于**减法,值 = 2147483645。
#   直译成 Python 的 `1 << 31 - 3` 会被解析成 `1 << 28` = 268435456 —— Python 的移位
#   优先级**低于**减法。这一个括号的差别足以让 Python 副本拒掉 Go 能接受的阵营 ID,
#   而且不报错,只表现为"某些对局在 Python 副本上分配失败"。
#
# 值的来历(照抄 Go 注释):与 DS Camp 编码边界一致 —— Camp 0/1 保留,玩家阵营写为
# faction+2;限制到 MaxInt32-2 保证 UE 侧 int32 Camp 转换永不溢出。
MAX_COMBAT_FACTION_ID = (1 << 31) - 3

_UINT32_MAX = (1 << 32) - 1
_UINT64_MAX = (1 << 64) - 1


def canonical_roster(player_ids: list[int]) -> tuple[list[int], str]:
    """返回(升序去重的 player IDs, 逗号分隔十进制 annotation)。

    对应 Go 的 CanonicalRoster。Go 返回 error 的每一处,这里都 raise ValueError
    (fail-closed 方向一致):空 roster、含 0、去重后超过 128 人。

    ★ 上限判定发生在**去重之后**(照抄 Go 的语句顺序):130 个 ID 里有 5 个重复时,
      Go 判的是 125 ≤ 128 → 通过。先判长度再去重会误拒这种输入。
    """
    if not player_ids:
        raise ValueError("battle roster must be non-empty")
    for raw in player_ids:
        # Go 的形参是 []uint64,负数 / 超 64 位在那边不可能存在。Python 不挡的话,
        # 一个负 ID 会一路走到 annotation 里变成 "-1",DS 解析出一个不存在的玩家。
        if not isinstance(raw, int) or raw < 0 or raw > _UINT64_MAX:
            raise ValueError(f"battle roster player_id {raw!r} out of uint64 range")
    # 显式 sorted:Python 的 list/dict "有序" 指的是插入序,不是排好序。
    # Go 那边是 sort.Slice,这里必须同样显式排,不能指望调用方传进来就是有序的。
    ids = sorted(player_ids)
    unique: list[int] = []
    for pid in ids:
        # 0 的检查在去重之前、且在**排序后**的循环里 —— 与 Go 逐字一致。
        # player_id=0 意味着上游丢了身份,把它送进 DS 会创建一个无主 Pawn。
        if pid == 0:
            raise ValueError("battle roster contains zero player_id")
        if unique and unique[-1] == pid:
            continue
        unique.append(pid)
    if len(unique) > MAX_BATTLE_ROSTER_PLAYERS:
        raise ValueError("battle roster exceeds 128 players")
    return unique, ",".join(str(pid) for pid in unique)


def canonical_combat_factions(
    player_ids: list[int], faction_by_player: dict[int, int]
) -> tuple[list[int], str]:
    """校验 roster → match-local 战斗阵营的**一一映射**,返回(canonical roster, annotation)。

    对应 Go 的 CanonicalCombatFactions。annotation 形如 `7=3,42=3,99=9`,
    按 player_id 升序 —— 顺序来自 canonical_roster,不是 dict 的插入序。
    (Python dict 保插入序,但那不是排序;直接 iterate faction_by_player 会得到
     一个随调用方构造顺序变化的串,与 Go 对不上。)

    ★ "精确覆盖"是三层判据,少一层就有洞:
        ① 条数相等   —— 挡住"多给了一个不在 roster 里的玩家"
        ② 逐个能查到 —— 挡住"条数对上了但键错位"(1,2 对 1,3)
        ③ 值不越界   —— 挡住 DS 侧 Camp 转换溢出
      Go 用的比较基准是**去重后**的 canonical 长度,不是入参长度;
      拿入参长度比会让"传了重复 ID"的调用莫名失败。
    """
    canonical_players, _ = canonical_roster(player_ids)
    if len(faction_by_player) != len(canonical_players):
        raise ValueError("combat factions must exactly cover battle roster")
    parts: list[str] = []
    for player_id in canonical_players:
        if player_id not in faction_by_player:
            raise ValueError("combat factions missing battle roster player")
        faction_id = faction_by_player[player_id]
        # Go 的 map 值类型是 uint32:负数不可能存在,越界在下一行拒。
        if not isinstance(faction_id, int) or faction_id < 0 or faction_id > _UINT32_MAX:
            raise ValueError(f"combat faction_id {faction_id!r} out of uint32 range")
        if faction_id > MAX_COMBAT_FACTION_ID:
            raise ValueError("combat faction_id exceeds DS camp range")
        parts.append(f"{player_id}={faction_id}")
    return canonical_players, ",".join(parts)
