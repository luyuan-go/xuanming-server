"""ds_allocator 服务 Go/Python 对照探针。

两个实现写**同一个 Redis**,所以按 base 参数分配不相交的 match_id / player_id 段。

★ 本探针的特殊性:**分配是一次性资源占用,不像 owner 那样能反复重置**。
所以每条场景用**自己的 match_id**(`BASE + n`),不复用 —— 复用的话第二条永远
落在 `allocate_idempotent_hit` 快路径上,diff 是零但什么都没验(README 规矩①,
owner 探针第一版就栽在这)。

前置:两侧必须以 `mode: "mock"` 起(确定性假地址)。`dev` 默认 `local`,
会真去 exec 一个 Windows DS 进程 —— 两个实现各 exec 一份,端口互抢,
diff 里全是"谁抢到端口"的噪声,跟实现分叉无关。
用法见 tools/parity/README.md。
"""

import asyncio
import re
import sys

import pathlib as _pl

# 按**文件位置**解析,不依赖调用时的 cwd —— 探针会被从各种目录调起。
_ROOT = _pl.Path(__file__).resolve().parents[2]   # python/
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "gen"))

import grpc
from google.protobuf import text_format

from pandora.common.v1 import errcode_pb2 as ec
from pandora.config.v1 import level_pb2 as lvl
from pandora.ds.v1 import allocator_pb2 as dpb
from pandora.ds.v1 import allocator_pb2_grpc as dgrpc

PORT = sys.argv[1]
BASE = int(sys.argv[2])  # match_id / player_id 段起点,两个实现互不相交

MAP_ID = 1001          # 关卡表里存在的 PVP 图
MAP_ID_ABSENT = 65000  # 关卡表里不存在

# ★ 写死 canonical UUIDv4,不能 uuid4() 现铸(现铸则两次运行输出必不同)。
OP1 = "cccccccc-1111-4111-8111-cccccccccccc"
OP2 = "dddddddd-2222-4222-8222-dddddddddddd"


def norm(text: str, ids: dict) -> str:
    """归一化随实现 / 时刻变化但不影响语义的值。

    盖掉的只有「必然不同」的四类:
      · match_id / player_id —— 两侧段不同
      · ds_addr / ds_pod_name —— mock 模式下由 match_id 派生,段不同则必然不同;
        但**是否为空**要保留(空地址 = 分配失败却回了 OK,是最该抓的分叉)
      · gameserver_uid / allocation_id —— 服务端自生成
      · 绝对时间戳

    刻意**不**盖的:code、instance_epoch、release_track、departed、status、state ——
    这些正是要验的判定结果(规矩③)。
    """
    def rep(prefix):
        def _f(m):
            v = int(m.group(1))
            return f"{prefix}: <{prefix[0].upper()}%d>" % ids.setdefault((prefix, v), len(ids) + 1)
        return _f

    text = re.sub(r"match_id: (\d+)", rep("match_id"), text)
    text = re.sub(r"player_id: (\d+)", rep("player_id"), text)
    # 地址 / pod 只判空非空 —— 值必然不同,但「该给的时候给了没有」必须验。
    for k in ("ds_addr", "ds_pod_name", "gameserver_uid", "allocation_id"):
        text = re.sub(rf'{k}: "([^"]*)"',
                      lambda m, k=k: f'{k}: <{"PRESENT" if m.group(1) else "EMPTY"}>',
                      text)
    for k in ("allocated_at_ms", "last_heartbeat_ms", "ts_ms"):
        text = re.sub(rf"{k}: (\d+)",
                      lambda m, k=k: f"{k}: <TS>" if int(m.group(1)) else f"{k}: 0",
                      text)
    return text


class Probe:
    def __init__(self):
        self.ids: dict = {}

    def dump(self, tag, resp, extra=""):
        # README 规矩①:每条都把 code 打出来自查,不能只依赖 diff ——
        # 只看 diff 发现不了"两边都错在同一处"。
        print(f"--- {tag}")
        print(f"    code={ec.ErrCode.Name(resp.code)}{extra}")
        body = text_format.MessageToString(resp, as_utf8=True).rstrip()
        body = "\n".join(ln for ln in body.splitlines() if not ln.startswith("code:"))
        print("\n".join("    " + ln for ln in norm(body, self.ids).splitlines()) or "    <empty>")

    def dump_battles(self, tag, resp, mine: set[int]):
        """ListBattles 只打**本段自己的**对局。

        战斗镜像按 match_id 建行,上一轮留下的行会被这一轮读到(规矩②);
        但这里比 hub 简单 —— match_id 本身就带段,过滤掉别人的即可,
        不需要像 probe_hub 那样做基线增量。
        """
        print(f"--- {tag}")
        print(f"    code={ec.ErrCode.Name(resp.code)}")
        rows = sorted((b for b in resp.battles if b.match_id in mine),
                      key=lambda b: b.match_id)
        for b in rows:
            print(f"    match=<M> state={b.state} player_count={b.player_count} "
                  f"addr={'PRESENT' if b.ds_addr else 'EMPTY'}")
        if not rows:
            print("    <本段无对局>")


def faction(pid: int, fid: int):
    return dpb.BattlePlayerCombatFaction(player_id=pid, combat_faction_id=fid)


async def allocate_ready(st, req, match_id: int | None = None, budget_sec: float = 25.0):
    """发起 AllocateBattle,并**同时扮演那台 DS** 把这一局推到 ready。

    ★ 不这么做的话整份探针一条都跑不完:`AllocateBattle` 会一直阻塞到 DS 用正确的
    match_id/pod 上报 ready/running 心跳为止,而 mock 模式下**根本没有真 DS 会上报**,
    于是必然挂满 `ready_wait_timeout`(dev 配 300s)。实测(2026-08-22)Go / Python
    两侧同样挂死 —— 这是探针自己的缺口,不是实现分叉(README 规矩①的另一种形态:
    第一条场景就卡住,后面 42 条压根没被执行,而"没跑"很容易被读成"没差异")。

    mock 模式下 pod 名由 match_id 派生(`pandora-battle-{match_id}`),所以探针能精确
    地只给自己这一局发心跳,不碰别的运行留下的行(规矩②)。

    心跳次数随两侧调度快慢而不同,但心跳响应**不进 diff** —— 进 diff 的只有最终那条
    AllocateBattle 响应,所以时序差异不会污染比对。
    """
    if match_id is None:
        match_id = req.match_id
    # ★ 绑成局部名:main() 里**每一条** AllocateBattle 都走本函数(哪条请求会通过校验、
    # 进而卡在 ready 等待上,不该由探针作者去猜 —— 猜错就是又一次 300s 挂死)。
    # 这里若仍写成 st.AllocateBattle(...),就会变成自我递归。
    invoke = st.AllocateBattle
    # grpc.aio 的 stub 调用返回的是 UnaryUnaryCall(可 await 的对象),**不是** coroutine,
    # 直接丢给 create_task 会 TypeError。包一层再交出去。
    call = invoke(req)

    async def _await_call():
        return await call

    task = asyncio.create_task(_await_call())
    pod = f"pandora-battle-{match_id}"
    loop = asyncio.get_running_loop()
    deadline = loop.time() + budget_sec
    while not task.done() and loop.time() < deadline:
        await asyncio.sleep(0.15)
        if task.done():
            break
        try:
            await st.Heartbeat(
                dpb.HeartbeatRequest(ds_pod_name=pod, match_id=match_id, state="running")
            )
        except grpc.aio.AioRpcError:
            # 记录还没落到 warming 时心跳会被拒 —— 正常,下一轮再试。
            pass
    return await task


async def main():
    async with grpc.aio.insecure_channel(f"127.0.0.1:{PORT}") as ch:
        st = dgrpc.DSAllocatorServiceStub(ch)
        p = Probe()
        A = dpb.AllocateBattleRequest

        m1, m2 = BASE + 1, BASE + 2
        team_a = [BASE + 101, BASE + 102]
        team_b = [BASE + 103, BASE + 104]
        roster = team_a + team_b
        fac = [faction(x, 0) for x in team_a] + [faction(x, 1) for x in team_b]
        mine: set[int] = set()

        # ── AllocateBattle ────────────────────────────────────────────────
        p.dump("01 allocate 正常 5v5",
               await allocate_ready(st, A(match_id=m1, player_ids=roster, map_id=MAP_ID,
                                          game_mode="5v5_ranked",
                                          player_combat_factions=fac,
                                          rating_mode=lvl.LEVEL_RATING_MODE_ELO,
                                          rating_pool="ranked_5v5"), m1))
        mine.add(m1)
        # ★ 幂等命中:同 match_id 重复分配**必须**回同一实例,不能再分配一台。
        p.dump("02 allocate 重复(幂等命中)",
               await allocate_ready(st, A(match_id=m1, player_ids=roster, map_id=MAP_ID,
                                          game_mode="5v5_ranked",
                                          player_combat_factions=fac,
                                          rating_mode=lvl.LEVEL_RATING_MODE_ELO,
                                          rating_pool="ranked_5v5")))
        # ★ 同 match_id 但 roster 变了。这是最容易分叉的一条:一侧仍走幂等回原实例,
        # 一侧发现 roster 不同判冲突。放行的话名单外的玩家会拿到票进场。
        p.dump("03 allocate 同 match 不同 roster",
               await allocate_ready(st, A(match_id=m1, player_ids=[BASE + 199], map_id=MAP_ID,
                                          game_mode="5v5_ranked")))

        p.dump("04 allocate match_id=0",
               await allocate_ready(st, A(player_ids=roster, map_id=MAP_ID,
                                          game_mode="5v5_ranked")))
        p.dump("05 allocate 空 roster",
               await allocate_ready(st, A(match_id=BASE + 3, map_id=MAP_ID,
                                          game_mode="5v5_ranked")))
        # ★ 关卡表里不存在的 map_id 必须拒。放行的话 DS 会起在一张不存在的图上,
        # 玩家永远卡加载(§9.20 不得让玩家进不去场景)。
        #
        # ⚠️ 但**本条在 mock 模式下恒回 OK,两侧一致,不是分叉也不是缺陷**:
        # 关卡表只在 mode=local/agones 起真 DS、按 map_id 拼地图命令行时才被查
        # (见 ds_allocator-dev.yaml config_table 段注释);mock 不解析地图,自然无从拒。
        # 2026-08-22 实测 Go / Python 同为 OK。要真验这条判定必须换 local 模式,
        # 而 local 会让两个实现互抢 DS 端口(见本文件开头)—— 即"对拍覆盖不到的一格",
        # 别把它读成"实现放行了非法地图"。07 map_id=0 / 08 game_mode 为空同理。
        p.dump("06 allocate map_id 不在关卡表",
               await allocate_ready(st, A(match_id=BASE + 4, player_ids=roster,
                                          map_id=MAP_ID_ABSENT, game_mode="5v5_ranked")))
        p.dump("07 allocate map_id=0",
               await allocate_ready(st, A(match_id=BASE + 5, player_ids=roster,
                                          game_mode="5v5_ranked")))
        p.dump("08 allocate game_mode 为空",
               await allocate_ready(st, A(match_id=BASE + 6, player_ids=roster, map_id=MAP_ID)))
        # ★ faction 列表与 roster 对不上。proto 明写「必须对 roster 每名玩家精确提供一条」,
        # 空列表只为兼容旧 matchmaker —— 那么"给了但只给一半"该怎么处理是分叉点。
        p.dump("09 allocate faction 只覆盖一半 roster",
               await allocate_ready(st, A(match_id=BASE + 7, player_ids=roster, map_id=MAP_ID,
                                          game_mode="5v5_ranked",
                                          player_combat_factions=fac[:2])))
        p.dump("10 allocate faction 含 roster 外的玩家",
               await allocate_ready(st, A(match_id=BASE + 8, player_ids=roster, map_id=MAP_ID,
                                          game_mode="5v5_ranked",
                                          player_combat_factions=fac + [faction(BASE + 199, 0)])))
        # ★ ELO 但 rating_pool 为空:proto 说这只可能出现在滚动升级期的旧 matchmaker,
        # battle_result 会按旧口径兜底 —— 那 allocator 这一层到底拒不拒?
        p.dump("11 allocate ELO 但 rating_pool 为空",
               await allocate_ready(st, A(match_id=BASE + 9, player_ids=roster, map_id=MAP_ID,
                                          game_mode="5v5_ranked",
                                          player_combat_factions=fac,
                                          rating_mode=lvl.LEVEL_RATING_MODE_ELO)))
        p.dump("12 allocate roster 内重复玩家",
               await allocate_ready(st, A(match_id=BASE + 10, player_ids=roster + [roster[0]],
                                          map_id=MAP_ID, game_mode="5v5_ranked")))

        # 单人副本(§17:与 5v5 同一条链,只是 team_size=1 + 另一个池)
        p.dump("13 allocate 单人 PVE",
               await allocate_ready(st, A(match_id=m2, player_ids=[BASE + 105], map_id=MAP_ID,
                                          game_mode="pve_coop",
                                          player_combat_factions=[faction(BASE + 105, 0)],
                                          rating_mode=lvl.LEVEL_RATING_MODE_NONE), m2))
        mine.add(m2)

        p.dump_battles("14 本段对局列表", await st.ListBattles(dpb.ListBattlesRequest()), mine)
        p.dump_battles("15 列表按 state 过滤",
                       await st.ListBattles(dpb.ListBattlesRequest(state_filter="ready")), mine)
        p.dump_battles("16 列表 state 过滤值非法",
                       await st.ListBattles(dpb.ListBattlesRequest(state_filter="no-such-state")),
                       mine)

        # ── ResolveBattleTarget(重连重签票的只读权威查询)─────────────────
        R = dpb.ResolveBattleTargetRequest
        p.dump("17 resolve 在 roster 内",
               await st.ResolveBattleTarget(R(match_id=m1, player_id=roster[0])))
        # ★ 不在 roster 内必须拒。放行 = 任何人报一个 match_id 就能拿到该局 DS 地址,
        # 再配合重签票就进了别人的对局。
        p.dump("18 resolve 不在 roster 内",
               await st.ResolveBattleTarget(R(match_id=m1, player_id=BASE + 199)))
        p.dump("19 resolve match 不存在",
               await st.ResolveBattleTarget(R(match_id=BASE + 777, player_id=roster[0])))
        p.dump("20 resolve match_id=0",
               await st.ResolveBattleTarget(R(player_id=roster[0])))
        p.dump("21 resolve player_id=0", await st.ResolveBattleTarget(R(match_id=m1)))

        # ── Heartbeat ─────────────────────────────────────────────────────
        HB = dpb.HeartbeatRequest
        # ★ 未知 pod 必须拒。放行的话任何人都能凭空造一个对局镜像/续 TTL,
        # 心跳超时补偿(不变量 §4:15s → abandoned → 段位回滚)就被绕过了。
        p.dump("22 heartbeat 未知 pod",
               await st.Heartbeat(HB(ds_pod_name=f"ghost-{BASE}", match_id=m1,
                                     state="running", player_count=4)))
        p.dump("23 heartbeat pod 为空",
               await st.Heartbeat(HB(match_id=m1, state="running")))
        p.dump("24 heartbeat match_id=0",
               await st.Heartbeat(HB(ds_pod_name=f"ghost-{BASE}", state="running")))
        p.dump("25 heartbeat state 非法",
               await st.Heartbeat(HB(ds_pod_name=f"ghost-{BASE}", match_id=m1,
                                     state="no-such-state")))
        # ★ census 的三个字段是一组。proto 明写「旧 DS 零值绝不能解释为『全员已离场』」——
        # present=false 却带着名单、或 present=true 名单却是空,都是离场证明造假的入口。
        p.dump("26 heartbeat census present=false 但带名单",
               await st.Heartbeat(HB(ds_pod_name=f"ghost-{BASE}", match_id=m1, state="running",
                                     active_player_ids=roster,
                                     active_player_snapshot_present=False,
                                     player_census_capability_version=1,
                                     player_census_id="probe-census-1")))
        p.dump("27 heartbeat census present=true 名单为空",
               await st.Heartbeat(HB(ds_pod_name=f"ghost-{BASE}", match_id=m1, state="running",
                                     active_player_snapshot_present=True,
                                     player_census_capability_version=1,
                                     player_census_id="probe-census-2")))
        p.dump("28 heartbeat census capability_version=0",
               await st.Heartbeat(HB(ds_pod_name=f"ghost-{BASE}", match_id=m1, state="running",
                                     active_player_ids=roster,
                                     active_player_snapshot_present=True,
                                     player_census_id="probe-census-3")))
        p.dump("29 heartbeat census_id 为空",
               await st.Heartbeat(HB(ds_pod_name=f"ghost-{BASE}", match_id=m1, state="running",
                                     active_player_ids=roster,
                                     active_player_snapshot_present=True,
                                     player_census_capability_version=1)))

        # ── EnsurePlayerDeparture(Battle→Hub 物理离场门)──────────────────
        E = dpb.EnsurePlayerDepartureRequest
        # ★ 实例元组必须完整。proto 明写「调用方必须携带从 placement/source snapshot
        # 取得的完整实例元组」—— 缺字段时一侧拒、一侧当 departed 返回,
        # 就等于放第二台 DS 进场(§9.22 一人一 DS)。
        p.dump("30 departure 缺实例元组",
               await st.EnsurePlayerDeparture(E(match_id=m1, player_id=roster[0],
                                                operation_id=OP1, placement_version=1)))
        p.dump("31 departure match 不存在",
               await st.EnsurePlayerDeparture(E(match_id=BASE + 777, player_id=roster[0],
                                                operation_id=OP1, ds_pod_name=f"ghost-{BASE}",
                                                gameserver_uid=f"uid-{BASE}", instance_epoch=1,
                                                allocation_id=f"alloc-{BASE}",
                                                placement_version=1,
                                                source_placement_version=1,
                                                source_operation_id=OP2)))
        p.dump("32 departure 不在 roster 内",
               await st.EnsurePlayerDeparture(E(match_id=m1, player_id=BASE + 199,
                                                operation_id=OP1, ds_pod_name=f"ghost-{BASE}",
                                                gameserver_uid=f"uid-{BASE}", instance_epoch=1,
                                                allocation_id=f"alloc-{BASE}",
                                                placement_version=1,
                                                source_placement_version=1,
                                                source_operation_id=OP2)))
        p.dump("33 departure operation_id 为空",
               await st.EnsurePlayerDeparture(E(match_id=m1, player_id=roster[0],
                                                ds_pod_name=f"ghost-{BASE}",
                                                gameserver_uid=f"uid-{BASE}", instance_epoch=1,
                                                allocation_id=f"alloc-{BASE}",
                                                placement_version=1)))

        # ── AbortPreactiveBattle(matchmaker 分配 saga 的补偿路径)──────────
        AB = dpb.AbortPreactiveBattleRequest
        p.dump("34 abort 未分配的 match",
               await st.AbortPreactiveBattle(AB(match_id=BASE + 778,
                                                allocation_operation_id=OP1)))
        p.dump("35 abort 缺实例元组",
               await st.AbortPreactiveBattle(AB(match_id=m2, allocation_operation_id=OP1)))
        p.dump("36 abort operation_id 为空",
               await st.AbortPreactiveBattle(AB(match_id=m2, ds_pod_name=f"ghost-{BASE}")))

        # ── ReleaseBattle ─────────────────────────────────────────────────
        RB = dpb.ReleaseBattleRequest
        # ★ authority_mode=redis 下「禁止按 match_id 临时回读当前实例后回收」——
        # 只给 match_id + reason 的裸调用该被拒。放行 = 任何人能回收任意在跑的对局。
        p.dump("37 release 只给 match+reason(裸调用)",
               await st.ReleaseBattle(RB(match_id=m2, reason="completed")))
        p.dump("38 release reason 非法",
               await st.ReleaseBattle(RB(match_id=m2, reason="no-such-reason")))
        p.dump("39 release match 不存在",
               await st.ReleaseBattle(RB(match_id=BASE + 779, reason="completed")))
        p.dump("40 release match_id=0", await st.ReleaseBattle(RB(reason="completed")))
        p.dump("41 release 带完整持久证明",
               await st.ReleaseBattle(RB(match_id=m2, reason="completed",
                                         allocation_id=f"alloc-{BASE}",
                                         ds_pod_name=f"ghost-{BASE}",
                                         gameserver_uid=f"uid-{BASE}", instance_epoch=1)))
        p.dump("42 release 重复(幂等)",
               await st.ReleaseBattle(RB(match_id=m2, reason="completed",
                                         allocation_id=f"alloc-{BASE}",
                                         ds_pod_name=f"ghost-{BASE}",
                                         gameserver_uid=f"uid-{BASE}", instance_epoch=1)))

        p.dump_battles("43 收尾:本段对局列表",
                       await st.ListBattles(dpb.ListBattlesRequest()), mine)


asyncio.run(main())
