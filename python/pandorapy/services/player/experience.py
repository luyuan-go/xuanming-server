"""玩家等级经验结算 —— 对应 Go 侧 internal/data/experience_repo.go 的 AdvanceExperience。

设计(realtime-progression.md §4.2):

  - 经验来源(battle_result 出箱 / 任务完成点 / GM)**只报 delta**,
    等级曲线的唯一权威在本服务 —— DS / 调用方不可信(§9.6)。
  - 入账与推送出箱**同一 MySQL 事务**;后台发布器轮询出箱投 kafka。
  - 多副本发布器**无需 claim / fencing**:事件是入账后的**全量权威快照**,
    客户端按 (level, exp_in_level) 单调不回退去重 —— 重复投递 / 旧快照晚到
    都无副作用(at-least-once + 快照语义)。

★ 进位循环的四个边界,每个都是"写错了不报错"的:

  1. **满级 no-op**:已满级时原样返回且级内经验恒 0,不再累加。
     漏了会让满级玩家的 exp 无限涨,升级表现反复播。
  2. **升到满级的瞬间级内经验清 0**:溢出的经验**不保留**。
     保留的话玩家满级后经验条是满的,视觉上像"还能再升"。
  3. **曲线项为 0 视为不可升级**(非法配置防御):停止进位而不是无限循环。
     不停的话一个配错的 0 会让循环转到天荒地老。
  4. **加法回绕按"足够升满"处理**:delta 有上限本不该发生,但防御性处理
     比让等级回绕成负数强。
"""

from __future__ import annotations

_UINT64_MAX = 2**64 - 1


def advance_experience(
    level: int, exp_in_level: int, delta: int, curve: list[int]
) -> tuple[int, int, int]:
    """级内经验加 delta 后按曲线循环进位。返回 (新等级, 新级内经验, 升级数)。

    curve[i] = 从 (i+1) 级升到 (i+2) 级所需经验。
    max_level = len(curve) + 1 —— 曲线有 N 项就能升到 N+1 级。

    ★ 纯函数,无副作用 —— 事务层只负责锁行和落库。
    这样它可以被跨语言对拍(见 tests/test_player_experience.py)。
    """
    max_level = len(curve) + 1

    # 防御:等级列被写成 0 / 负数时按 1 级处理,而不是让下面的索引越界。
    if level < 1:
        level = 1

    # ★ ① 满级 no-op:不加经验,级内经验恒 0。
    if level >= max_level:
        return max_level, 0, 0

    exp = exp_in_level + delta
    # ★ ④ 回绕防御。Python 的 int 无限精度不会真回绕,但保持与 Go 同样的语义,
    # 让两边在"delta 极大"时结论一致(都按足够升满处理)。
    if exp > _UINT64_MAX:
        exp = _UINT64_MAX

    gained = 0
    while level < max_level:
        need = curve[level - 1]
        # ★ ③ 曲线项为 0 = 非法配置 → 停止进位(而不是无限循环)。
        if need == 0 or exp < need:
            break
        exp -= need
        level += 1
        gained += 1
        # ★ ② 升到满级的瞬间清 0,溢出经验不保留。
        if level >= max_level:
            exp = 0
            break

    return level, exp, gained


def decorate_experience(
    level: int, exp_in_level: int, curve: list[int], *, enabled: bool = True
) -> tuple[int, bool]:
    """给档案补经验派生字段(GetProfile 出参装饰)。返回 (展示用 exp, 是否满级)。

    满级 → is_max_level=True 且级内经验按 0 展示(权威列已保证满级恒 0,
    这里是防御性夹紧 —— 万一历史数据有残留,展示层也不该露出来)。

    功能关闭 / 曲线未配置 → 不标满级、exp 原样(行为与历史一致)。
    """
    if not enabled or not curve:
        return exp_in_level, False
    max_level = len(curve) + 1
    if level >= max_level:
        return 0, True
    return exp_in_level, False
