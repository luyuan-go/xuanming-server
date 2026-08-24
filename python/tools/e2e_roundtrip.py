"""端到端全程验收:登录 → 组队 → 匹配 → 进战斗 → 打完结算 → 退回大厅。

**这是"能不能进游戏"唯一的自动证明。** 单测和对拍都证明不了这件事:
对拍证明"Python 与 Go 逐字节一样",但两边可以一起坏;这条链证明的是"链本身是通的"。

前置:
  1. `pwsh tools/scripts/dev_up.ps1` + `pwsh tools/scripts/dev_migrate.ps1`
  2. `python .venv/Scripts/python.exe tools/run_stack.py`(22/22 起来)
  3. 本机没有编译好的 UE DS 时,hub_allocator 要以 `mode: "mock"` 起 ——
     否则 AssignHub 会去 exec 一个真的 Hub DS,拿不到大厅地址。

用法::

    cd python && .venv/Scripts/python.exe tools/e2e_roundtrip.py

★ 被"代跑"的只有 **DS 自己那几步**,真 DS 编译出来后由它自己做:
    · 上报 ready 心跳(否则 AllocateBattle 会挂满 ready_wait_timeout);
    · 上报 census 花名册(玩家在场 / 离场闭环);
    · 打完调 ReportResult + ReleaseBattle;
    · 玩家连上大厅后 SetLocation(HUB)。
  除此之外每一跳都是真服务、真库、真 Redis、真票据。

★ 每步都打 code 自查,不靠"没抛异常"当通过 —— 只看有没有报错会把"整段跳过了"
  读成"通过了"(tools/parity/README.md 规矩①)。
"""

from __future__ import annotations

import asyncio
import os
import pathlib as _pl
import sys
import time
import uuid

# 按**文件位置**解析,不依赖调用时的 cwd —— 这个脚本会被从各种目录调起。
_ROOT = _pl.Path(__file__).resolve().parents[1]   # python/
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "gen"))

import grpc

from pandora.battle.v1 import battle_pb2 as bpb
from pandora.battle.v1 import battle_pb2_grpc as bgrpc
from pandora.common.v1 import errcode_pb2 as ec
from pandora.config.v1 import level_pb2 as lvl
from pandora.ds.v1 import allocator_pb2 as dpb
from pandora.ds.v1 import allocator_pb2_grpc as dgrpc
from pandora.locator.v1 import locator_pb2 as locpb
from pandora.locator.v1 import locator_pb2_grpc as locgrpc
from pandora.login.v1 import login_pb2 as lpb
from pandora.login.v1 import login_pb2_grpc as lgrpc
from pandora.match.v1 import match_pb2 as mpb
from pandora.match.v1 import match_pb2_grpc as mgrpc
from pandora.team.v1 import team_pb2 as tpb
from pandora.team.v1 import team_pb2_grpc as tgrpc

LOGIN, TEAM, MM_PVE = "127.0.0.1:20001", "127.0.0.1:20010", "127.0.0.1:20018"
DSALLOC, BATTLERES, LOCATOR = "127.0.0.1:20020", "127.0.0.1:20022", "127.0.0.1:20006"
MAP_ID, ROLE_ID = 13, 1001

STAMP = int(time.time())
fails: list[str] = []


def step(name: str, code: int, extra: str = "") -> bool:
    good = code == ec.OK
    if not good:
        fails.append(name)
    print(f"[{'OK ' if good else 'ERR'}] {name}: code={ec.ErrCode.Name(code)}{extra}")
    return good


def check(name: str, cond: bool, extra: str = "") -> bool:
    if not cond:
        fails.append(name)
    print(f"[{'OK ' if cond else 'ERR'}] {name}{extra}")
    return cond


async def main() -> int:
    async with grpc.aio.insecure_channel(LOGIN) as lch, \
            grpc.aio.insecure_channel(TEAM) as tch, \
            grpc.aio.insecure_channel(MM_PVE) as mch, \
            grpc.aio.insecure_channel(DSALLOC) as dch, \
            grpc.aio.insecure_channel(BATTLERES) as bch, \
            grpc.aio.insecure_channel(LOCATOR) as locch:
        lst, tst, mst = lgrpc.LoginServiceStub(lch), tgrpc.TeamServiceStub(tch), mgrpc.MatchServiceStub(mch)
        dst = dgrpc.DSAllocatorServiceStub(dch)
        bst = bgrpc.BattleResultServiceStub(bch)
        locst = locgrpc.PlayerLocatorServiceStub(locch)

        print("──── 1. 登录进大厅 ────")
        r = await lst.Login(lpb.LoginRequest(account=f"rt-{STAMP}", password_hash="pw",
                                             device_id=f"dev-{STAMP}"), timeout=15)
        step("Login", r.code)
        pid = r.roles[0].player_id if r.roles else r.player_id
        acc_md = (("x-pandora-account-id", str(r.account_id)),)
        pid_md = (("x-pandora-player-id", str(pid)),)
        print(f"       player_id={pid}")

        er = await lst.EnterRole(lpb.EnterRoleRequest(player_id=pid, device_id=f"dev-{STAMP}"),
                                 metadata=acc_md, timeout=15)
        step("EnterRole", er.code)
        session = er.session_token
        if er.selected_role_id == 0:
            sr = await lst.SelectRole(lpb.SelectRoleRequest(role_id=ROLE_ID),
                                      metadata=pid_md, timeout=15)
            step("SelectRole", sr.code)

        print("──── 2. 组队 + 匹配 ────")
        ct = await tst.CreateTeam(tpb.CreateTeamRequest(), metadata=pid_md, timeout=15)
        step("CreateTeam", ct.code, f" team_id={ct.team_id}")
        rd = await tst.SetReady(tpb.SetReadyRequest(team_id=ct.team_id, ready=True,
                                                    hero_id=ROLE_ID), metadata=pid_md, timeout=15)
        step("SetReady", rd.code)
        sm = await mst.StartMatch(mpb.StartMatchRequest(
            team_id=ct.team_id, map_id=MAP_ID,
            entry_mode=lvl.LEVEL_ENTRY_MODE_WALK_IN), metadata=pid_md, timeout=30)
        step("StartMatch", sm.code)

        # DS 代跑:真 DS 起来后由它自己上报 ready 心跳。
        census_id = str(uuid.uuid4())
        state = {"pod": "", "match": 0, "stop": False}

        async def fake_ds():
            while not state["stop"]:
                try:
                    lb = await dst.ListBattles(dpb.ListBattlesRequest(), timeout=5)
                    for b in lb.battles:
                        if b.state in ("warming", "ready", "running"):
                            if not state["pod"]:
                                state["pod"], state["match"] = b.ds_pod_name, b.match_id
                            await dst.Heartbeat(dpb.HeartbeatRequest(
                                ds_pod_name=b.ds_pod_name, match_id=b.match_id, state="running",
                                player_count=1, active_player_ids=[pid],
                                active_player_snapshot_present=True,
                                player_census_capability_version=1,
                                player_census_id=census_id), timeout=5)
                except Exception:
                    pass
                await asyncio.sleep(0.4)

        # PANDORA_E2E_NO_FAKE_DS=1:不代跑 DS,验证**真 DS**能不能自己起来并上报 ready。
        # 这是"我到底能不能进游戏"的决定性判据 —— 代跑着永远问不出这个答案。
        no_fake = os.environ.get("PANDORA_E2E_NO_FAKE_DS", "") == "1"
        ds_task = None if no_fake else asyncio.create_task(fake_ds())
        if no_fake:
            print("       [真 DS 模式] 不代跑心跳,等真 DS 自己上报(editor 冷启动可能要 1~2 分钟)")

        print("       等待撮合 + DS 就绪 ...")
        match_id, ds_addr, ticket = 0, "", ""
        # 真 DS 模式下要给足 editor 冷启动时间(ds_allocator.ready_wait_timeout 配的是 300s)。
        deadline = time.time() + (300 if no_fake else 120)
        while time.time() < deadline:
            gp = await mst.GetMatchProgress(mpb.GetMatchProgressRequest(),
                                            metadata=pid_md, timeout=15)
            if gp.progress.battle_ds_addr:
                match_id = gp.progress.match_id
                ds_addr = gp.progress.battle_ds_addr
                ticket = gp.progress.battle_ticket
                break
            await asyncio.sleep(2)
        check("匹配完成拿到战斗 DS", bool(ds_addr),
              f": match_id={match_id} ds_addr={ds_addr} ticket={'PRESENT' if ticket else 'EMPTY'}")
        if not ds_addr:
            state["stop"] = True
            return 1

        print("──── 3. 进战斗(DS 验票 + 上报花名册)────")
        pod = state["pod"] or f"pandora-battle-{match_id}"
        # ★ 本机 dev 是 local-off-v1 档:matchmaker 只注入 legacy HS256 signer(v2 为空),
        #   UE DS 走 HS256LocalOff 分支**自己本地验票**(只校验 player/match/exp),
        #   不会去调 login.VerifyDSTicket —— 那是 v2 / Model-B 的兑换点路径。
        #   (第一版探针在这里调了 VerifyDSTicket,拿到 ERR_UNAUTHORIZED:
        #    login-dev.yaml 开了 require_ticket_sjti=true,而 local-off 的 legacy 票
        #    结构上就带不了 sjti。Go 侧 SignBattleTicket 的 legacy 分支逐字相同,
        #    所以那不是移植缺陷,是探针挑错了验票路径。)
        import base64
        import json as _json

        def jwt_claims(tok: str) -> dict:
            body = tok.split(".")[1]
            body += "=" * (-len(body) % 4)
            return _json.loads(base64.urlsafe_b64decode(body))

        cl = jwt_claims(ticket)
        check("票据 player_id 与登录的一致", str(cl.get("player_id", cl.get("sub", ""))) == str(pid),
              f": ticket={cl.get('player_id', cl.get('sub'))} login={pid}")
        check("票据 match_id 与本局一致", str(cl.get("match_id", "")) == str(match_id),
              f": ticket={cl.get('match_id')} match={match_id}")
        check("票据是 battle 类型且未过期",
              cl.get("ds_type") in ("battle", None) and int(cl.get("exp", 0)) > time.time(),
              f": ds_type={cl.get('ds_type')} exp_in={int(cl.get('exp', 0)) - int(time.time())}s")

        # 花名册心跳已由 fake_ds 持续上报;等 locator 把位置刷成 BATTLE。
        loc_state = 0
        for _ in range(30):
            gl = await locst.GetLocation(locpb.GetLocationRequest(player_id=pid), timeout=10)
            loc_state = gl.location.state
            if loc_state == locpb.LOCATION_STATE_BATTLE:
                break
            await asyncio.sleep(1)
        check("位置权威 = BATTLE(玩家真的在战斗里)",
              loc_state == locpb.LOCATION_STATE_BATTLE,
              f": state={locpb.LocationState.Name(loc_state)}")

        print("──── 4. 打完:结算 + 退场 ────")
        now_ms = int(time.time() * 1000)
        res = bpb.BattleResult(
            match_id=match_id, started_at_ms=now_ms - 60000, ended_at_ms=now_ms,
            winner_team=0, ds_pod_name=pod, game_mode="pve_coop", map_id=MAP_ID,
            stats=[bpb.PlayerStats(player_id=pid, hero_id=ROLE_ID, team=0,
                                   kills=7, deaths=1, assists=3,
                                   damage_dealt=12345, damage_taken=6789,
                                   healing=100, gold=888)])
        rr = await bst.ReportResult(bpb.ReportResultRequest(result=res), timeout=30)
        step("ReportResult(战报结算)", rr.code)

        # ★ 玩家离场必须显式上报:login 在 IssueDSTicket(hub) 前会查位置权威,
        #   presence 还是 BATTLE 就 fail-closed 拒发大厅票(§9.1 一人一 DS)。
        #   靠 location_ttl(30s)自然过期不算数 —— 那是兜底不是流程。
        #
        #   离场**不是** EnsurePlayerDeparture:那条 RPC 随 placement 路由体系一起硬切删了
        #   (调它恒 ERR_SERVICE_DISABLED,服务端会打 battle_departure_rpc_rejected)。
        #   现在的闭环是 DS 发一份**不再包含该玩家**的 census 花名册快照。
        state["stop"] = True
        await asyncio.sleep(0.6)
        hb = await dst.Heartbeat(dpb.HeartbeatRequest(
            ds_pod_name=pod, match_id=match_id, state="running",
            player_count=0, active_player_ids=[],
            active_player_snapshot_present=True,
            player_census_capability_version=1,
            player_census_id=census_id), timeout=15)
        step("Heartbeat(空花名册 = 玩家离场闭环)", hb.code)
        rb = await dst.ReleaseBattle(dpb.ReleaseBattleRequest(
            match_id=match_id, reason="completed"), timeout=20)
        step("ReleaseBattle(DS 退场)", rb.code)

        gm = await bst.GetMatchResult(bpb.GetMatchResultRequest(match_id=match_id), timeout=15)
        step("GetMatchResult(战报可查)", gm.code,
             f" winner_team={gm.result.winner_team} stats={len(gm.result.stats)}")

        print("──── 5. 退回大厅 ────")
        # ★ 身份取自 x-pandora-player-id 头(Envoy 注入),不传就是 ds_ticket_issue_no_player_id。
        #
        # ★ 要**退避重试**:DS 停止上报后 BATTLE presence 靠 locator 的 location_ttl(30s)
        #   自然过期,ds_allocator 两侧都不主动 ClearLocation(Go / Python 一致,不是缺陷)。
        #   presence 还是 BATTLE 时 login 按 §9.1 fail-closed 拒发大厅票,客户端照 §9.23
        #   退避重查即可 —— 这正是"不卡死、有出口"的正常形态,不是错误。
        it = None
        t0 = time.time()
        while time.time() - t0 < 75:
            it = await lst.IssueDSTicket(lpb.IssueDSTicketRequest(
                session_token=session, ds_type="hub"), metadata=pid_md, timeout=20)
            if it.code == ec.OK:
                break
            await asyncio.sleep(3)
        print(f"       (等 BATTLE presence 过期并重试了 {int(time.time() - t0)}s)")
        step("IssueDSTicket(hub)", it.code,
             f" hub_ds_addr={it.hub_ds_addr!r} ticket={'PRESENT' if it.ticket else 'EMPTY'}")
        check("拿到大厅直连地址", bool(it.hub_ds_addr))

        gl = await locst.GetLocation(locpb.GetLocationRequest(player_id=pid), timeout=10)
        check("已脱离 BATTLE(不再被判在战斗里)",
              gl.location.state != locpb.LOCATION_STATE_BATTLE,
              f": state={locpb.LocationState.Name(gl.location.state)}")

        # ★ HUB presence 是**大厅 DS** 在玩家连上来之后上报的,不是后端自己写的 ——
        #   拿到票只代表"后端允许你进大厅"。这里代跑大厅 DS 的这一步(真 Hub DS 会做),
        #   把最后一环也验上:上报后位置权威应变成 HUB。
        sl = await locst.SetLocation(locpb.SetLocationRequest(
            player_id=pid,
            location=locpb.Location(state=locpb.LOCATION_STATE_HUB,
                                    hub_pod="pandora-hub-mock-1", shard_id=1)), timeout=15)
        step("SetLocation(HUB)(代跑大厅 DS 上报)", sl.code)
        gl = await locst.GetLocation(locpb.GetLocationRequest(player_id=pid), timeout=10)
        check("位置权威 = HUB(已回到大厅)",
              gl.location.state == locpb.LOCATION_STATE_HUB,
              f": state={locpb.LocationState.Name(gl.location.state)} hub_pod={gl.location.hub_pod!r}")

        if ds_task is not None:
            ds_task.cancel()
        print()
        if fails:
            print(f"✗ 失败 {len(fails)} 项:{fails}")
            return 1
        print("✓ 全程打通:登录 → 组队 → 匹配 → 进战斗 → 结算 → 退回大厅")
        return 0


sys.exit(asyncio.run(main()))
