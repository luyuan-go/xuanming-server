"""mission 服务 Go/Python 对照探针(真进程 + 真 MySQL / Redis / Kafka)。

用法::

    # Python 版(端口 20119),player_id 段起点 7500000
    PYTHONUTF8=1 .venv/Scripts/python.exe tools/parity/probe_mission.py 20119 7500000

    # Go 版(端口 20019),换一个不相交的段
    PYTHONUTF8=1 .venv/Scripts/python.exe tools/parity/probe_mission.py 20019 7600000

    diff /tmp/py.txt /tmp/go.txt && echo 零差异

两个实现写**同一个库**(pandora_mission),所以 player_id 必须分段:
`mission_reward_log.grant_idempotency_key` = ``mission:<player_id>:<mission_id>``、
`mission_fact_receipts` 的 uk 是 ``(player_id, idempotency_key)`` ——
段重叠时第二次跑会撞上第一次的收据,表现为 already 恒 true、进度不动,
看起来像"实现分叉",实际是探针自己脏了。

四条写探针的规矩(见同目录 README):
  ① 每个断言前先证明前置状态(本探针的 `expect` 参数):
     只 diff 不自查时,"什么都没测到但全绿"是最常见的失败模式。
  ② 跨运行共享的资源分段(player_id + 幂等键都拼 BASE)。
  ③ 归一化只盖"必然不同"的:player_id / 绝对毫秒时间戳 / 幂等键里的段号。
     错误码、进度值、reward_state 一律**原样打印**——那些正是要验的东西。
  ④ 每条结论都要能被反向注入验证(把被测那行改坏,确认这一行变红)。

配置表依赖(configtable/dist,读表决定本探针的期望值):
    60001 初出茅庐  type=1/sub=1  cond 61001(杀怪 x5)  next=60002  reward 62001  手动领
    60002 战场拾荒  type=1/sub=1  cond 61002(拾取 x3)               reward 62002  自动发
    60003 里程碑    type=2/sub=0  cond 61003(完成 60001 x1)         reward 62003  自动发
表变了就要改本文件的期望值——**别把期望值算成动态读表**,那样探针会跟着实现一起错。
"""

from __future__ import annotations

import asyncio
import pathlib as _pl
import re
import sys

# 按**文件位置**解析,不依赖调用时的 cwd —— 探针会被从各种目录调起。
_ROOT = _pl.Path(__file__).resolve().parents[2]  # python/
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "gen"))

import grpc  # noqa: E402

from pandora.common.v1 import errcode_pb2 as ec  # noqa: E402
from pandora.mission.v1 import mission_pb2 as m  # noqa: E402
from pandora.mission.v1 import mission_pb2_grpc as mg  # noqa: E402

PORT = sys.argv[1]
BASE = int(sys.argv[2])

# dev yaml 里的 MySQL 连接串(不复制凭据:直接读那份**未改动的** yaml)。
DEV_YAML = _ROOT.parent / "services" / "social" / "mission" / "etc" / "mission-dev.yaml"

# 任务配置 ID(读自 configtable/dist,见模块 docstring)。
MI_KILL = 60001  # 手动领奖 + next=60002
MI_PICK = 60002  # 自动发奖,与 60001 同 (type,sub_type) → 互斥
MI_MILE = 60003  # 条件 = 完成 60001(完成扇出再入的唯一验证点)
MI_GHOST = 69999  # 表里不存在

CAT_KILL = int(m.MISSION_CONDITION_CATEGORY_KILL_MONSTER)  # 1
CAT_PICKUP = int(m.MISSION_CONDITION_CATEGORY_PICKUP_ITEM)  # 9

# 玩家分段:每条独立剧情一个玩家,互不污染。
P_CHAIN = BASE + 1  # 主链:接取→进度→完成扇出→领奖
P_ABANDON = BASE + 2  # 放弃 / 重接
P_GM = BASE + 3  # CompleteAllMissions
P_ARG = BASE + 4  # 参数与鉴权边界
P_GUARD = BASE + 5  # 空槽 / amount=0 护栏

_ALL_PLAYERS = (P_CHAIN, P_ABANDON, P_GM, P_ARG, P_GUARD)

_TS_KEYS = ("accepted_at_ms", "completed_at_ms", "created_at_ms", "updated_at_ms")

_fail_count = 0


def md(player_id: int) -> tuple[tuple[str, str], ...]:
    """客户端面 metadata:Envoy jwt_authn 注入的身份头(本机直连时手工带)。"""
    return (
        ("x-pandora-player-id", str(player_id)),
        ("x-request-id", f"probe-mission-{player_id}"),
    )


SYS: tuple[tuple[str, str], ...] = (("x-request-id", "probe-mission-sys"),)
"""系统面 metadata:**不带** player-id —— 带了就是越权,service 层 systemOnly 应拒。"""


def key(name: str) -> str:
    """幂等键拼上段号:同键重复上报会被收据吸收,不分段第二次跑就永远 already=true。"""
    return f"probe:{BASE}:{name}"


def norm(text: str) -> str:
    """归一化"必然不同"的值,其余逐字节保留。

    只盖三类:player_id(两实现用不同段)、绝对毫秒时间戳、幂等键里的段号。
    错误码 / 进度 / targets / reward_state / already 全部原样 —— 它们是被测对象。
    """
    ids: dict[int, str] = {}
    for i, pid in enumerate(_ALL_PLAYERS, 1):
        ids[pid] = f"<P{i}>"

    def _pid(mo: re.Match) -> str:
        v = int(mo.group(2))
        return f"{mo.group(1)}{ids.get(v, '<P?%d>' % v)}"

    text = re.sub(r"(player_id[=: ]+)(\d+)", _pid, text)
    for k in _TS_KEYS:
        text = re.sub(rf"{k}: \d+", f"{k}: <TS>", text)
    text = text.replace(f"probe:{BASE}:", "probe:<SEG>:")
    text = text.replace(f"mission:{BASE}", "mission:<SEG>")
    return text


def out(line: str = "") -> None:
    print(norm(line))


def code_of(resp) -> str:  # noqa: ANN001
    return ec.ErrCode.Name(resp.code)


def check(tag: str, got: str, expect: str | None) -> None:
    """打印一步的结果,并**当场自查**是否走到了预期分支。

    expect=None 表示"这一步只是铺场景,不是断言点"。
    自查失败不中断:后续步骤的实际行为同样是证据,提前 return 会把它们全丢掉。
    """
    global _fail_count
    if expect is None:
        out(f"    {tag}: {got}")
        return
    ok = got == expect
    if not ok:
        _fail_count += 1
    out(f"    {tag}: {got}   [{'OK' if ok else 'MISMATCH expect=' + expect}]")


def fmt_active(a) -> str:  # noqa: ANN001
    return (
        f"active(mission={a.mission_config_id} "
        f"progress={list(a.progress)} targets={list(a.targets)} "
        f"accepted_at_ms: {a.accepted_at_ms})"
    )


def fmt_done(d) -> str:  # noqa: ANN001
    return (
        f"done(mission={d.mission_config_id} "
        f"reward_state={m.MissionRewardState.Name(d.reward_state)} "
        f"completed_at_ms: {d.completed_at_ms})"
    )


async def snapshot(st, player_id: int, tag: str, expect: str | None = None) -> str:
    """ListMissions 快照 —— 既是断言点,也是"事实真的落库了"的证据。

    返回一个稳定的单行摘要,便于用 expect 精确钉住。
    """
    r = await st.ListMissions(m.ListMissionsRequest(), metadata=md(player_id))
    act = sorted(r.active, key=lambda a: a.mission_config_id)
    dn = sorted(r.completed, key=lambda d: d.mission_config_id)
    summary = " ".join(
        [f"code={code_of(r)}"]
        + [f"A{a.mission_config_id}={list(a.progress)}/{list(a.targets)}" for a in act]
        + [f"D{d.mission_config_id}={m.MissionRewardState.Name(d.reward_state)}" for d in dn]
    )
    check(tag, summary, expect)
    for a in act:
        out(f"        {fmt_active(a)}")
    for d in dn:
        out(f"        {fmt_done(d)}")
    return summary


def facts(*specs: tuple[int, tuple[int, ...], int]) -> list[m.MissionFact]:
    return [
        m.MissionFact(condition_category=c, condition_ids=list(s), amount=n)
        for c, s, n in specs
    ]


async def report(st, player_id: int, idem: str, *specs, metadata=SYS):  # noqa: ANN001
    return await st.ReportMissionFacts(
        m.ReportMissionFactsRequest(
            player_id=player_id, facts=facts(*specs), idempotency_key=idem
        ),
        metadata=metadata,
    )


# ── 主链 ────────────────────────────────────────────────────────────────────


async def scene_chain(st) -> None:  # noqa: ANN001, PLR0915
    out("=== 01 主链:接取 → 进度 → 完成扇出 → 领奖")
    p = P_CHAIN

    # ★ 前置证明:这个段必须是干净的。不打这一条的话,残留状态会让下面每个
    #   期望值都对不上,而报错指向的是"实现不一致"而不是"探针脏了"。
    await snapshot(st, p, "01.1 起始快照(必须为空)", "code=OK")

    r = await st.AcceptMission(m.AcceptMissionRequest(mission_config_id=MI_KILL), metadata=md(p))
    check("01.2 接取 60001", f"{code_of(r)} {fmt_active(r.mission)}", None)
    check(
        "01.2b 初始快照",
        f"{code_of(r)} progress={list(r.mission.progress)} targets={list(r.mission.targets)}",
        "OK progress=[0] targets=[5]",
    )

    r = await st.AcceptMission(m.AcceptMissionRequest(mission_config_id=MI_KILL), metadata=md(p))
    check("01.3 重复接取 60001", code_of(r), "ERR_MISSION_ALREADY_ACCEPTED")

    # ★ (type=1,sub_type=1) 与 60001 相同 —— 必须在 60001 **活跃时**试,
    #   60001 一旦完成就离开活跃列表,互斥闸压根不会被求值(那时的 OK 说明不了任何事)。
    r = await st.AcceptMission(m.AcceptMissionRequest(mission_config_id=MI_PICK), metadata=md(p))
    check("01.4 接取同类型 60002(互斥)", code_of(r), "ERR_MISSION_TYPE_CONFLICT")

    r = await st.AcceptMission(m.AcceptMissionRequest(mission_config_id=MI_MILE), metadata=md(p))
    check("01.5 接取异类型 60003", code_of(r), "OK")

    r = await st.AcceptMission(m.AcceptMissionRequest(mission_config_id=MI_GHOST), metadata=md(p))
    check("01.6 接取不存在任务", code_of(r), "ERR_MISSION_CONFIG_NOT_FOUND")

    r = await st.AcceptMission(m.AcceptMissionRequest(mission_config_id=0), metadata=md(p))
    check("01.7 接取 id=0", code_of(r), "ERR_INVALID_ARG")

    await snapshot(
        st, p, "01.8 两条活跃", "code=OK A60001=[0]/[5] A60003=[0]/[1]"
    )

    r = await report(st, p, key("k1"), (CAT_KILL, (7001,), 2))
    check("01.9 上报杀怪 x2", f"{code_of(r)} already={r.already}", "OK already=False")
    # ★ 进度必须**回读**确认:ReportMissionFacts 返回 OK 只说明事务提交了,
    #   不说明这条事实匹配上了条件行(槽位过滤不命中时同样返回 OK)。
    await snapshot(st, p, "01.10 进度 2/5", "code=OK A60001=[2]/[5] A60003=[0]/[1]")

    r = await report(st, p, key("k1"), (CAT_KILL, (7001,), 2))
    check("01.11 同键同内容重放", f"{code_of(r)} already={r.already}", "OK already=True")
    await snapshot(st, p, "01.12 幂等未双计", "code=OK A60001=[2]/[5] A60003=[0]/[1]")

    r = await report(st, p, key("k1"), (CAT_KILL, (7001,), 3))
    check("01.13 同键不同内容", code_of(r), "ERR_MISSION_FACTS_CONFLICT")

    # 触发完成扇出:60001 满 5 → 完成(手动领 → CLAIMABLE)→ next 自动接 60002
    #                → COMPLETE_MISSION(60001) 再入 → 60003 达标 → 自动发奖(NONE)
    r = await report(st, p, key("k2"), (CAT_KILL, (7001,), 9))
    check("01.14 上报杀怪 x9(超额)", f"{code_of(r)} already={r.already}", "OK already=False")
    await snapshot(
        st,
        p,
        "01.15 完成扇出",
        "code=OK A60002=[0]/[3] D60001=MISSION_REWARD_STATE_CLAIMABLE "
        "D60003=MISSION_REWARD_STATE_NONE",
    )

    r = await st.ClaimMissionReward(
        m.ClaimMissionRewardRequest(mission_config_id=MI_KILL), metadata=md(p)
    )
    check("01.16 领奖 60001", code_of(r), "OK")
    await snapshot(
        st,
        p,
        "01.17 领奖后",
        "code=OK A60002=[0]/[3] D60001=MISSION_REWARD_STATE_CLAIMED "
        "D60003=MISSION_REWARD_STATE_NONE",
    )

    r = await st.ClaimMissionReward(
        m.ClaimMissionRewardRequest(mission_config_id=MI_KILL), metadata=md(p)
    )
    check("01.18 重复领奖", code_of(r), "ERR_MISSION_NOT_CLAIMABLE")

    r = await st.ClaimMissionReward(
        m.ClaimMissionRewardRequest(mission_config_id=MI_MILE), metadata=md(p)
    )
    check("01.19 领 auto_reward 已发的 60003", code_of(r), "ERR_MISSION_NOT_CLAIMABLE")

    r = await st.ClaimMissionReward(
        m.ClaimMissionRewardRequest(mission_config_id=MI_PICK), metadata=md(p)
    )
    check("01.20 领未完成的 60002", code_of(r), "ERR_MISSION_NOT_CLAIMABLE")

    r = await st.ClaimMissionReward(
        m.ClaimMissionRewardRequest(mission_config_id=MI_GHOST), metadata=md(p)
    )
    check("01.21 领不存在任务", code_of(r), "ERR_MISSION_NOT_CLAIMABLE")

    r = await st.ClaimMissionReward(
        m.ClaimMissionRewardRequest(mission_config_id=0), metadata=md(p)
    )
    check("01.22 领 id=0", code_of(r), "ERR_INVALID_ARG")

    # 自动接的 60002 走拾取条件完成 → auto_reward=1 → 直接 NONE(不经 CLAIMABLE)
    r = await report(st, p, key("k3"), (CAT_PICKUP, (10001,), 3))
    check("01.23 上报拾取 x3", f"{code_of(r)} already={r.already}", "OK already=False")
    await snapshot(
        st,
        p,
        "01.24 60002 自动发奖完成",
        "code=OK D60001=MISSION_REWARD_STATE_CLAIMED "
        "D60002=MISSION_REWARD_STATE_NONE D60003=MISSION_REWARD_STATE_NONE",
    )

    r = await st.AbandonMission(m.AbandonMissionRequest(mission_config_id=MI_KILL), metadata=md(p))
    check("01.25 放弃已完成任务", code_of(r), "ERR_MISSION_ALREADY_COMPLETED")

    r = await st.AcceptMission(m.AcceptMissionRequest(mission_config_id=MI_KILL), metadata=md(p))
    check("01.26 重接已完成任务", code_of(r), "ERR_MISSION_ALREADY_COMPLETED")
    out()


# ── 放弃 / 重接 ──────────────────────────────────────────────────────────────


async def scene_abandon(st) -> None:  # noqa: ANN001
    out("=== 02 放弃 / 重接")
    p = P_ABANDON
    await snapshot(st, p, "02.1 起始快照(必须为空)", "code=OK")

    r = await st.AcceptMission(m.AcceptMissionRequest(mission_config_id=MI_KILL), metadata=md(p))
    check("02.2 接取 60001", code_of(r), "OK")

    r = await report(st, p, key("a1"), (CAT_KILL, (7001,), 3))
    check("02.3 推进到 3/5", f"{code_of(r)} already={r.already}", "OK already=False")
    await snapshot(st, p, "02.4 放弃前", "code=OK A60001=[3]/[5]")

    r = await st.AbandonMission(m.AbandonMissionRequest(mission_config_id=MI_KILL), metadata=md(p))
    check("02.5 放弃", code_of(r), "OK")
    # ★ 回读证明行真的删了:只看 Abandon 返回 OK 的话,"删了 0 行也返回 OK"这类
    #   实现分叉是看不出来的。
    await snapshot(st, p, "02.6 放弃后为空", "code=OK")

    r = await st.AbandonMission(m.AbandonMissionRequest(mission_config_id=MI_KILL), metadata=md(p))
    check("02.7 重复放弃", code_of(r), "ERR_MISSION_NOT_ACCEPTED")

    r = await st.AbandonMission(m.AbandonMissionRequest(mission_config_id=MI_GHOST), metadata=md(p))
    check("02.8 放弃不存在任务", code_of(r), "ERR_MISSION_NOT_ACCEPTED")

    r = await st.AbandonMission(m.AbandonMissionRequest(mission_config_id=0), metadata=md(p))
    check("02.9 放弃 id=0", code_of(r), "ERR_INVALID_ARG")

    r = await st.AcceptMission(m.AcceptMissionRequest(mission_config_id=MI_KILL), metadata=md(p))
    check("02.10 放弃后可重接", code_of(r), "OK")
    # 进度必须从 0 重来(放弃 = 丢进度,不是暂存)
    await snapshot(st, p, "02.11 重接后进度归零", "code=OK A60001=[0]/[5]")
    out()


# ── GM 批量完成 ──────────────────────────────────────────────────────────────


async def scene_gm(st) -> None:  # noqa: ANN001
    out("=== 03 GM CompleteAllMissions(刻意不发奖 / 不接链 / 不再入)")
    p = P_GM
    await snapshot(st, p, "03.1 起始快照(必须为空)", "code=OK")

    for mid in (MI_KILL, MI_MILE):
        r = await st.AcceptMission(m.AcceptMissionRequest(mission_config_id=mid), metadata=md(p))
        check(f"03.2 接取 {mid}", code_of(r), "OK")
    await snapshot(st, p, "03.3 两条活跃", "code=OK A60001=[0]/[5] A60003=[0]/[1]")

    r = await st.CompleteAllMissions(
        m.CompleteAllMissionsRequest(player_id=p), metadata=SYS
    )
    check("03.4 GM 全完成", f"{code_of(r)} count={r.completed_count}", "OK count=2")
    # ★ 关键断言:GM 路径**不得**标记可领、不得自动接 60002。
    #   reward_state 全是 NONE 且活跃列表为空,才证明它没走完成扇出那条路。
    await snapshot(
        st,
        p,
        "03.5 GM 后:无可领 / 无自动接链",
        "code=OK D60001=MISSION_REWARD_STATE_NONE D60003=MISSION_REWARD_STATE_NONE",
    )

    r = await st.ClaimMissionReward(
        m.ClaimMissionRewardRequest(mission_config_id=MI_KILL), metadata=md(p)
    )
    check("03.6 GM 完成的任务不可领", code_of(r), "ERR_MISSION_NOT_CLAIMABLE")

    r = await st.CompleteAllMissions(
        m.CompleteAllMissionsRequest(player_id=p), metadata=SYS
    )
    check("03.7 再次 GM(无活跃)", f"{code_of(r)} count={r.completed_count}", "OK count=0")
    out()


# ── 参数 / 鉴权边界 ─────────────────────────────────────────────────────────


async def scene_args(st) -> None:  # noqa: ANN001
    out("=== 04 参数与鉴权边界")
    p = P_ARG

    # 客户端面:无身份 = 拒
    r = await st.ListMissions(m.ListMissionsRequest())
    check("04.1 ListMissions 无身份", code_of(r), "ERR_UNAUTHORIZED")
    r = await st.AcceptMission(m.AcceptMissionRequest(mission_config_id=MI_KILL))
    check("04.2 AcceptMission 无身份", code_of(r), "ERR_UNAUTHORIZED")
    r = await st.AbandonMission(m.AbandonMissionRequest(mission_config_id=MI_KILL))
    check("04.3 AbandonMission 无身份", code_of(r), "ERR_UNAUTHORIZED")
    r = await st.ClaimMissionReward(m.ClaimMissionRewardRequest(mission_config_id=MI_KILL))
    check("04.4 ClaimMissionReward 无身份", code_of(r), "ERR_UNAUTHORIZED")

    # ★ 系统面方向**相反**:有身份 = 拒。写反了不会报错,只会让玩家自己给自己
    #   上报事实刷满任务 —— 所以必须正着反着各测一次。
    r = await report(st, p, key("deny"), (CAT_KILL, (1,), 1), metadata=md(p))
    check("04.5 ReportMissionFacts 带玩家身份", code_of(r), "ERR_PERMISSION_DENY")
    r = await st.CompleteAllMissions(
        m.CompleteAllMissionsRequest(player_id=p), metadata=md(p)
    )
    check("04.6 CompleteAllMissions 带玩家身份", code_of(r), "ERR_PERMISSION_DENY")

    r = await report(st, 0, key("p0"), (CAT_KILL, (1,), 1))
    check("04.7 Report player_id=0", code_of(r), "ERR_INVALID_ARG")
    r = await st.ReportMissionFacts(
        m.ReportMissionFactsRequest(player_id=p, idempotency_key=key("nofact")),
        metadata=SYS,
    )
    check("04.8 Report facts 为空", code_of(r), "ERR_INVALID_ARG")
    r = await report(st, p, "", (CAT_KILL, (1,), 1))
    check("04.9 Report 幂等键为空", code_of(r), "ERR_INVALID_ARG")

    # 幂等键长度边界:128 放行 / 129 拒(Go: len(idemKey) > 128)
    k128 = ("x" * (128 - len(key("")))) and (key("") + "x" * (128 - len(key(""))))
    r = await report(st, p, k128, (CAT_KILL, (1,), 1))
    check("04.10 幂等键 128 字节", f"{code_of(r)} len={len(k128)}", "OK len=128")
    r = await report(st, p, k128 + "y", (CAT_KILL, (1,), 1))
    check("04.11 幂等键 129 字节", code_of(r), "ERR_INVALID_ARG")

    # 单次事实条数上限(默认 64)
    r = await st.ReportMissionFacts(
        m.ReportMissionFactsRequest(
            player_id=p,
            facts=facts(*[(CAT_KILL, (1,), 1)] * 65),
            idempotency_key=key("f65"),
        ),
        metadata=SYS,
    )
    check("04.12 单次 65 条事实", code_of(r), "ERR_INVALID_ARG")
    r = await st.ReportMissionFacts(
        m.ReportMissionFactsRequest(
            player_id=p,
            facts=facts(*[(CAT_KILL, (1,), 1)] * 64),
            idempotency_key=key("f64"),
        ),
        metadata=SYS,
    )
    check("04.13 单次 64 条事实", code_of(r), "OK")

    r = await st.CompleteAllMissions(m.CompleteAllMissionsRequest(player_id=0), metadata=SYS)
    check("04.14 CompleteAll player_id=0", code_of(r), "ERR_INVALID_ARG")
    out()


# ── 引擎护栏 ────────────────────────────────────────────────────────────────


async def scene_guards(st) -> None:  # noqa: ANN001
    out("=== 05 引擎护栏:空槽 / amount=0 / 类别不匹配")
    p = P_GUARD
    await snapshot(st, p, "05.1 起始快照(必须为空)", "code=OK")

    r = await st.AcceptMission(m.AcceptMissionRequest(mission_config_id=MI_KILL), metadata=md(p))
    check("05.2 接取 60001", code_of(r), "OK")

    # 条件 61001 四个槽全空 = 匹配任意杀怪事实。所以"空 slot_values 不推进"这条护栏
    # **只能在这种全空槽条件上验**:换成有槽过滤的条件,不推进也可能是过滤没命中。
    r = await report(st, p, key("g_empty"), (CAT_KILL, (), 3))
    check("05.3 空 slot_values 上报", f"{code_of(r)} already={r.already}", "OK already=False")
    await snapshot(st, p, "05.4 空槽不推进", "code=OK A60001=[0]/[5]")

    r = await report(st, p, key("g_zero"), (CAT_KILL, (7001,), 0))
    check("05.5 amount=0 上报", f"{code_of(r)} already={r.already}", "OK already=False")
    await snapshot(st, p, "05.6 amount=0 不推进", "code=OK A60001=[0]/[5]")

    r = await report(st, p, key("g_cat"), (CAT_PICKUP, (7001,), 3))
    check("05.7 类别不匹配(拾取事实 vs 杀怪条件)", f"{code_of(r)} already={r.already}", "OK already=False")
    await snapshot(st, p, "05.8 类别不匹配不推进", "code=OK A60001=[0]/[5]")

    # ★ 反证:同一段里换成"类别匹配 + 非空槽 + amount>0"必须推进。
    #   没有这一条,上面三条"不推进"可能只是因为整条上报链路根本没通。
    r = await report(st, p, key("g_ok"), (CAT_KILL, (7001,), 1))
    check("05.9 正例(应推进)", f"{code_of(r)} already={r.already}", "OK already=False")
    await snapshot(st, p, "05.10 正例推进 1/5", "code=OK A60001=[1]/[5]")

    # 超额进度按目标 clamp(不是累加到 99)
    r = await report(st, p, key("g_clamp"), (CAT_KILL, (7001,), 99))
    check("05.11 超额上报", f"{code_of(r)} already={r.already}", "OK already=False")
    await snapshot(
        st, p, "05.12 clamp 到目标并完成", "code=OK A60002=[0]/[3] D60001=MISSION_REWARD_STATE_CLAIMABLE"
    )
    out()


# ── 落库核对(RPC 之外的证据面)──────────────────────────────────────────────


async def scene_db() -> None:
    """直接查 MySQL:证明 RPC 返回的状态确实落到了权威表,不是只在内存里对。

    发奖流水 status 只做**存在性**核对,不钉 PENDING/GRANTED —— 补扫循环随时
    会把它从 0 翻成 1,钉死了会让探针随跑随红(而这跟实现分叉无关)。
    """
    out("=== 06 落库核对(直查 pandora_mission)")
    try:
        import asyncmy  # noqa: PLC0415

        from pandorapy import mysqlx  # noqa: PLC0415
        from pandorapy.services.mission import conf as mconf  # noqa: PLC0415
    except Exception as exc:  # noqa: BLE001
        out(f"    SKIP: 依赖缺失 {exc}")
        return
    try:
        cfg = mconf.Config.load(DEV_YAML)
        conn_cfg = mysqlx.parse_go_dsn(cfg.node.mysql_client.dsn, default_db="pandora_mission")
        conn = await asyncmy.connect(
            host=conn_cfg["host"],
            port=conn_cfg["port"],
            user=conn_cfg["user"],
            password=conn_cfg["password"],
            db=conn_cfg["db"],
            autocommit=True,
        )
    except Exception as exc:  # noqa: BLE001
        out(f"    SKIP: 连不上库 {exc}")
        return
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT player_id, mission_config_id FROM player_mission_active "
                "WHERE player_id BETWEEN %s AND %s ORDER BY player_id, mission_config_id",
                (BASE, BASE + 99),
            )
            for pid, mid in await cur.fetchall():
                out(f"    active_row player_id={pid} mission={mid}")
            await cur.execute(
                "SELECT player_id, mission_config_id, reward_state FROM player_mission_done "
                "WHERE player_id BETWEEN %s AND %s ORDER BY player_id, mission_config_id",
                (BASE, BASE + 99),
            )
            for pid, mid, rs in await cur.fetchall():
                out(f"    done_row player_id={pid} mission={mid} reward_state={rs}")
            await cur.execute(
                "SELECT player_id, mission_config_id, grant_idempotency_key "
                "FROM mission_reward_log WHERE player_id BETWEEN %s AND %s "
                "ORDER BY player_id, mission_config_id",
                (BASE, BASE + 99),
            )
            for pid, mid, gk in await cur.fetchall():
                out(f"    reward_log player_id={pid} mission={mid} key={gk}")
            await cur.execute(
                "SELECT player_id, COUNT(*) FROM mission_fact_receipts "
                "WHERE player_id BETWEEN %s AND %s GROUP BY player_id ORDER BY player_id",
                (BASE, BASE + 99),
            )
            for pid, n in await cur.fetchall():
                out(f"    receipts player_id={pid} count={n}")
            await cur.execute(
                "SELECT COUNT(*) FROM mission_player_guards WHERE player_id BETWEEN %s AND %s",
                (BASE, BASE + 99),
            )
            (guards,) = await cur.fetchone()
            out(f"    guard_rows count={guards}")
    finally:
        conn.close()
    out()


async def main() -> None:
    out(f"# probe_mission port={PORT} seg=<SEG>")
    out()
    async with grpc.aio.insecure_channel(f"127.0.0.1:{PORT}") as ch:
        st = mg.MissionServiceStub(ch)
        await scene_chain(st)
        await scene_abandon(st)
        await scene_gm(st)
        await scene_args(st)
        await scene_guards(st)
    await scene_db()
    out(f"# self_check_mismatches={_fail_count}")


if __name__ == "__main__":
    asyncio.run(main())
    sys.exit(1 if _fail_count else 0)
