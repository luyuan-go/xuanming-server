"""hub_allocator 服务 Go/Python 对照探针。

两个实现写**同一个 Redis**,所以按 base 参数分配不相交的 player_id 段。

★ 本探针与 owner/dialogue 的关键差别:**分片镜像是跨运行共享的**。
`pandora:hub:shard:{pod}` 按 pod 名建行,不按 player_id 分区 —— 第一次运行留下的
`player_count` 会被第二次运行读到,ListHubs 的人数必然不同。README 规矩②说的就是这件事。

这里**不**把 player_count 盖成占位符(规矩③:归一化只能盖必然不同的,不能盖要验的),
而是改成打**相对本次运行基线的增量**。容量计数正确与否照样被逐字节比对,
而两次运行的绝对起点不同这件事被消掉了。

前置:hub_allocator 必须以 `mode: "mock"` 起(确定性假分片,不需要 Agones / 真 Hub DS)。
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
from pandora.hub.v1 import allocator_pb2 as hpb
from pandora.hub.v1 import allocator_pb2_grpc as hgrpc

PORT = sys.argv[1]
BASE = int(sys.argv[2])  # player_id 段起点,两个实现互不相交

REGION = "cn-east"

# ★ operation_id 必须写死成 canonical UUIDv4,不能 uuid4() 现铸 ——
# 现铸的话两次运行输出必然不同,diff 就废了(同 probe_owner)。
OP1 = "aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa"
OP2 = "bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb"

# 分片镜像基线:pod → 探针启动时的 player_count。
_baseline: dict[str, int] = {}


def norm(text: str, ids: dict) -> str:
    """归一化随实现 / 时刻变化但不影响语义的值。

    盖掉的只有三类「必然不同」:
      · player_id —— 两侧段不同
      · 票据 JWT —— 载荷含 jti / iat / exp,逐字节必不同;票据**内容**的正确性
        由 login VerifyDSTicket 侧的用例负责,这里只验"该给的时候给了、
        不该给的时候没给"(所以只判空 / 非空,不判具体值)
      · 绝对时间戳

    刻意**不**盖的:code、shard_id、line_no、capacity、is_full、is_current、
    state、departed、new_shard_id —— 这些正是要验的判定结果。
    """
    def rep_pid(m):
        v = int(m.group(1))
        return "player_id: <P%d>" % ids.setdefault(v, len(ids) + 1)

    text = re.sub(r"player_id: (\d+)", rep_pid, text)
    # 票据只判"有没有",不判内容(见 docstring)。
    text = re.sub(r'(hub_ticket|new_hub_ticket): "([^"]*)"',
                  lambda m: f'{m.group(1)}: <{"PRESENT" if m.group(2) else "EMPTY"}>',
                  text)
    for k in ("allocated_at_ms", "last_heartbeat_ms", "created_at_ms", "ts_ms"):
        text = re.sub(rf"{k}: (\d+)",
                      lambda m, k=k: f"{k}: <TS>" if int(m.group(1)) else f"{k}: 0",
                      text)
    return text


class Probe:
    def __init__(self, stub):
        self.stub = stub
        self.ids: dict[int, int] = {}

    def dump(self, tag, resp, extra=""):
        # ★ README 规矩①:每条都把实际 code 打出来自查。
        # 第一版 owner 探针三条 ★ 场景全落在幂等快路径上,diff 仍是零 —— 验了个寂寞。
        # 只看 diff 不看 code,就发现不了"两边都错在同一处"。
        print(f"--- {tag}")
        print(f"    code={ec.ErrCode.Name(resp.code)}{extra}")
        body = text_format.MessageToString(resp, as_utf8=True).rstrip()
        body = "\n".join(ln for ln in body.splitlines() if not ln.startswith("code:"))
        print("\n".join("    " + ln for ln in norm(body, self.ids).splitlines()) or "    <empty>")

    def dump_hubs(self, tag, resp):
        """ListHubs 专用:人数打**相对基线的增量**,而不是绝对值。

        绝对值跨运行必然不同(分片按 pod 建行,不按 player_id 分区);
        增量才是"本次分配 / 释放有没有正确记账"的可比证据。
        """
        print(f"--- {tag}")
        print(f"    code={ec.ErrCode.Name(resp.code)}")
        for h in sorted(resp.hubs, key=lambda x: (x.region, x.hub_pod_name)):
            delta = h.player_count - _baseline.get(h.hub_pod_name, 0)
            print(f"    pod=<POD> region={h.region} shard_state={h.state} "
                  f"capacity={h.capacity} count_delta={delta:+d}")
        if not resp.hubs:
            print("    <no hubs>")


def md(pid: int):
    return (("x-pandora-player-id", str(pid)), ("x-request-id", "probe-hub"))


async def main():
    async with grpc.aio.insecure_channel(f"127.0.0.1:{PORT}") as ch:
        st = hgrpc.HubAllocatorServiceStub(ch)
        p = Probe(st)

        # ── 基线:记下本次运行开始时各分片的人数 ──────────────────────────
        base_resp = await st.ListHubs(hpb.ListHubsRequest())
        for h in base_resp.hubs:
            _baseline[h.hub_pod_name] = h.player_count
        print(f"--- 00 基线分片数={len(base_resp.hubs)} "
              f"code={ec.ErrCode.Name(base_resp.code)}")

        A = hpb.AssignHubRequest
        pid = BASE + 1

        # ── AssignHub ─────────────────────────────────────────────────────
        p.dump("01 assign 基本", await st.AssignHub(A(player_id=pid, region=REGION, role_id=1)))
        p.dump("02 assign 重复(幂等?同一分片?)",
               await st.AssignHub(A(player_id=pid, region=REGION, role_id=1)))
        p.dump("03 assign player_id=0", await st.AssignHub(A(player_id=0, region=REGION, role_id=1)))
        p.dump("04 assign region 为空", await st.AssignHub(A(player_id=BASE + 2, role_id=1)))
        p.dump("05 assign region 不存在",
               await st.AssignHub(A(player_id=BASE + 3, region="no-such-region", role_id=1)))
        # ★ role_id=0 = "调用方不知角色,保留已存值"。新玩家没有已存值 ——
        # 这里要验的是它到底签不签票(签了就是把 role_id=0 写进票,选角权威被绕过)。
        p.dump("06 assign role_id=0 新玩家",
               await st.AssignHub(A(player_id=BASE + 4, region=REGION)))

        # ★ placement lease 三项必须**全有或全无**。只给一项是最容易实现分叉的地方:
        # 一侧 strict 拒绝、另一侧当没配置放行,两种行为在单元测试里都"合理"。
        p.dump("07 placement 只给 version(应拒)",
               await st.AssignHub(A(player_id=BASE + 5, region=REGION, role_id=1,
                                    placement_version=1)))
        p.dump("08 placement 只给 operation_id(应拒)",
               await st.AssignHub(A(player_id=BASE + 6, region=REGION, role_id=1,
                                    placement_operation_id=OP1)))
        p.dump("09 placement 三项齐全",
               await st.AssignHub(A(player_id=BASE + 7, region=REGION, role_id=1,
                                    placement_version=1, placement_operation_id=OP1,
                                    source_match_id=BASE + 900)))
        p.dump("10 assign 带 session_jti",
               await st.AssignHub(A(player_id=BASE + 8, region=REGION, role_id=1,
                                    session_jti="probe-sjti-fixed")))

        p.dump_hubs("11 分配后的分片人数增量", await st.ListHubs(hpb.ListHubsRequest()))

        # ── EnsureHubDepartureForBattle ───────────────────────────────────
        E = hpb.EnsureHubDepartureForBattleRequest
        # ★ 这道门的全部意义是"迟到的旧 op 不能顶掉新归属"。所以必须分别验
        # 版本对得上 / 对不上两条路径,只验成功路径等于没验 fencing。
        p.dump("12 departure 无归属的玩家",
               await st.EnsureHubDepartureForBattle(
                   E(player_id=BASE + 50, match_id=BASE + 901,
                     placement_version=1, placement_operation_id=OP1)))
        p.dump("13 departure 版本对不上(应拒/不 departed)",
               await st.EnsureHubDepartureForBattle(
                   E(player_id=BASE + 7, match_id=BASE + 900,
                     placement_version=999, placement_operation_id=OP2)))
        p.dump("14 departure 版本对得上",
               await st.EnsureHubDepartureForBattle(
                   E(player_id=BASE + 7, match_id=BASE + 900,
                     placement_version=1, placement_operation_id=OP1)))
        p.dump("15 departure 重复(幂等)",
               await st.EnsureHubDepartureForBattle(
                   E(player_id=BASE + 7, match_id=BASE + 900,
                     placement_version=1, placement_operation_id=OP1)))

        # ── 玩家侧线路 ────────────────────────────────────────────────────
        p.dump("16 ListHubLines 无鉴权", await st.ListHubLines(hpb.ListHubLinesRequest()))
        lines = await st.ListHubLines(hpb.ListHubLinesRequest(), metadata=md(pid))
        p.dump("17 ListHubLines(region 留空=服务端权威)", lines)
        p.dump("18 ListHubLines 显式 region",
               await st.ListHubLines(hpb.ListHubLinesRequest(region=REGION), metadata=md(pid)))
        p.dump("19 ListHubLines region 不存在",
               await st.ListHubLines(hpb.ListHubLinesRequest(region="no-such-region"),
                                     metadata=md(pid)))

        T = hpb.TransferToLineRequest
        p.dump("20 TransferToLine 无鉴权", await st.TransferToLine(T(target_shard_id=1)))
        p.dump("21 TransferToLine shard=0", await st.TransferToLine(T(), metadata=md(pid)))
        p.dump("22 TransferToLine 不存在的 shard",
               await st.TransferToLine(T(target_shard_id=60000), metadata=md(pid)))
        # ★ 切到"自己已经在的那条线"是最容易分叉的一条:一侧当 no-op 返回 OK 并重签票,
        # 另一侧当非法请求拒。两种都说得通,所以必须对拍。
        cur = next((ln.shard_id for ln in lines.lines if ln.is_current), 0)
        p.dump("23 TransferToLine 切到当前线路",
               await st.TransferToLine(T(target_shard_id=cur), metadata=md(pid)),
               extra=f"  (current_shard_exists={cur != 0})")
        other = next((ln.shard_id for ln in lines.lines
                      if ln.shard_id != cur and not ln.is_full), 0)
        if other:
            p.dump("24 TransferToLine 切到别的线路",
                   await st.TransferToLine(T(target_shard_id=other), metadata=md(pid)))
        else:
            print("--- 24 TransferToLine 切到别的线路\n    <只有一条线路,跳过>")

        # ── TransferHub(内部传送点)───────────────────────────────────────
        H = hpb.TransferHubRequest
        p.dump("25 TransferHub 无归属玩家",
               await st.TransferHub(H(player_id=BASE + 51, target_hub_id=1)))
        p.dump("26 TransferHub target=0", await st.TransferHub(H(player_id=pid)))

        # ── Heartbeat ─────────────────────────────────────────────────────
        # ★ 未知 pod 必须被拒。放行的话任何人都能凭空造一个分片进容量池,
        # 后续 AssignHub 会把玩家发到一个不存在的地址。
        HB = hpb.HeartbeatRequest
        p.dump("27 Heartbeat 未知 pod",
               await st.Heartbeat(HB(hub_pod_name=f"ghost-{BASE}", state="ready",
                                     player_count=1, max_players=500)))
        p.dump("28 Heartbeat pod 名为空",
               await st.Heartbeat(HB(state="ready", max_players=500)))
        known = base_resp.hubs[0].hub_pod_name if base_resp.hubs else ""
        if known:
            cap = base_resp.hubs[0].capacity
            p.dump("29 Heartbeat 已知 pod / max_players 与 capacity 一致",
                   await st.Heartbeat(HB(hub_pod_name=known, state="ready",
                                         player_count=0, max_players=cap)))
            # ★ Model B 明写:max_players 与 capacity 不等时不得刷新心跳 / 不得翻 ready。
            # 这是"allocator=500 / UE=16"那类容量假证明的唯一拦截点。
            p.dump("30 Heartbeat max_players 与 capacity 不等(应拒绝副作用)",
                   await st.Heartbeat(HB(hub_pod_name=known, state="ready",
                                         player_count=0, max_players=cap + 1)))
            p.dump("31 Heartbeat max_players=0(应拒)",
                   await st.Heartbeat(HB(hub_pod_name=known, state="ready", player_count=0)))
        else:
            print("--- 29..31 Heartbeat\n    <mock 未种出分片,跳过>")

        # ── ReleaseHub + 收尾 ─────────────────────────────────────────────
        p.dump("32 release", await st.ReleaseHub(hpb.ReleaseHubRequest(player_id=pid)))
        p.dump("33 release 重复(幂等)",
               await st.ReleaseHub(hpb.ReleaseHubRequest(player_id=pid)))
        p.dump("34 release 从未分配过的玩家",
               await st.ReleaseHub(hpb.ReleaseHubRequest(player_id=BASE + 52)))
        p.dump("35 release player_id=0", await st.ReleaseHub(hpb.ReleaseHubRequest()))

        # 把本探针占用的名额还回去,否则跑几轮就把 mock 分片占满,
        # 后续运行的 is_full / 分片选择结果会跟着变 —— 那不是实现分叉。
        for i in (2, 3, 4, 5, 6, 7, 8):
            await st.ReleaseHub(hpb.ReleaseHubRequest(player_id=BASE + i))
        p.dump_hubs("36 收尾后的分片人数增量(应全部回到 +0)",
                    await st.ListHubs(hpb.ListHubsRequest()))


asyncio.run(main())
