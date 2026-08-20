"""owner 服务 Go/Python 对照探针。

两个实现写**同一个库**,所以按 base 参数分配不相交的 player_id 段;
输出里把 player_id / 时间戳 / operation_id 归一化,再逐字节 diff。
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
from pandora.owner.v1 import owner_pb2 as opb
from pandora.owner.v1 import owner_pb2_grpc as ogrpc

PORT = sys.argv[1]
BASE = int(sys.argv[2])  # player_id 段起点,两个实现互不相交

OWNER_TYPE_HUB = 1
OWNER_TYPE_BATTLE = 2
PHASE_PENDING = 1

# ★ operation_id 必须是 canonical UUIDv4(§9.23:一次真实进场用一个**稳定**幂等键)。
# 这里写死三个,而不是 uuid4() 现铸 —— 现铸的话两次运行输出必然不同,diff 就废了。
OP1 = "11111111-1111-4111-8111-111111111111"
OP2 = "22222222-2222-4222-8222-222222222222"
OP3 = "33333333-3333-4333-8333-333333333333"

_TS_KEYS = ("admit_not_before_ms", "lease_deadline_ms", "updated_at_ms")


def norm(text: str, ids: dict) -> str:
    """归一化随实现/时刻变化但不影响语义的值。

    ⚠️ 只归一化「必然不同」的:player_id(段不同)、绝对时间戳。
    retry_after_ms 刻意**不**归一化成占位符 —— 它是本次要验的东西之一,
    只做量级分桶(见 bucket),否则这条 diff 就等于没验。
    """
    def rep_pid(m):
        v = int(m.group(1))
        return "player_id: <P%d>" % ids.setdefault(v, len(ids) + 1)

    text = re.sub(r"player_id: (\d+)", rep_pid, text)
    text = text.replace(f"-{BASE}", "-<SEG>")
    # 服务端在 operation_id 留空时**各自生成** UUID(两边行为一致,值必然不同)。
    # 只归一化 UUID 形态的,显式传入的 op-1/op-2 原样保留 —— 否则就把
    # 「有没有正确回显调用方传的 operation_id」也一起盖掉了。
    def _op(m):
        v = m.group(1)
        # 显式传入的固定 UUID 必须**原样保留** —— 否则「服务端有没有正确回显
        # 调用方的 operation_id」这件事就被归一化盖掉了(§9.23 幂等键必须可追溯)。
        return f'operation_id: "{v}"' if v in (OP1, OP2, OP3) else 'operation_id: "<GENERATED>"'

    text = re.sub(r'operation_id: "([0-9a-f-]{36})"', _op, text)
    # 正文里的 retry_after_ms 绝对值两边差几毫秒(墙钟),做量级归一;
    # 「是不是 0」由摘要行的 bucket() 单独断言,不会被这里盖掉。
    text = re.sub(r"retry_after_ms: (\d+)",
                  lambda m: "retry_after_ms: <%s>" % ("0" if int(m.group(1)) == 0 else "POSITIVE"),
                  text)
    for k in _TS_KEYS:
        text = re.sub(rf"{k}: (\d+)", lambda m, k=k: f"{k}: <TS>" if int(m.group(1)) else f"{k}: 0", text)
    return text


def bucket(ms: int) -> str:
    """把 retry_after_ms 分桶:0 / 1..2000 / >2000。

    精确值两边不可能相同(墙钟差几毫秒),但「是不是 0」和「量级对不对」
    正是要验的 —— 0 就意味着调用方拿不到退避时长。
    """
    if ms <= 0:
        return "0(★ 调用方无法退避)"
    if ms <= 2000:
        return "1..2000ms"
    return ">2000ms"


class Probe:
    def __init__(self, stub, ids):
        self.stub = stub
        self.ids = ids

    def dump(self, tag, resp, extra=""):
        print(f"--- {tag}")
        print(f"    code={ec.ErrCode.Name(resp.code)}{extra}")
        body = text_format.MessageToString(resp, as_utf8=True).rstrip()
        # code 已单独打过,正文里去掉避免重复
        body = "\n".join(ln for ln in body.splitlines() if not ln.startswith("code:"))
        print("\n".join("    " + ln for ln in norm(body, self.ids).splitlines()) or "    <empty>")


def target(pod, uid, epoch=1, assign="a-1", track="stable"):
    """★ instance_uid 必须按 BASE 分段。

    ds_instance_lease 是按 instance_uid 建行的,**不按 player_id 分区**。
    两个实现(乃至同一实现的两次运行)若共用 uid,上一轮留下的租约会被下一轮读到,
    表现是 lease_deadline_ms 时有时无 —— 看起来像实现分叉,其实是探针自己的残留。
    """
    return opb.OwnerTarget(
        pod_name=f"{pod}-{BASE}",
        instance_uid=f"{uid}-{BASE}",
        instance_epoch=epoch,
        assignment_or_allocation_id=assign,
        release_track=track,
    )


async def main():
    ids = {}
    async with grpc.aio.insecure_channel(f"127.0.0.1:{PORT}") as ch:
        stub = ogrpc.OwnerServiceStub(ch)
        p = Probe(stub, ids)
        pid = BASE + 1
        t1 = target("hub-a", "uid-a")
        t2 = target("hub-b", "uid-b", epoch=2, assign="a-2")

        p.dump("01 Query 全新玩家", await stub.QueryOwner(opb.QueryOwnerRequest(player_id=pid)))

        p.dump("02 Begin 首次(expect_epoch=0)", await stub.BeginTransition(
            opb.BeginTransitionRequest(player_id=pid, expect_epoch=0, operation_id=OP1,
                                       owner_type=OWNER_TYPE_HUB, target=t1, source_revision=1 << 24 | 1)))

        p.dump("03 Query 已建记录", await stub.QueryOwner(opb.QueryOwnerRequest(player_id=pid)))

        p.dump("04 Admit(首次归属,无旧租约→屏障应已开)", await stub.Admit(
            opb.AdmitRequest(player_id=pid, owner_epoch=1, operation_id=OP1, target=t1)))

        p.dump("05 Admit 重放(幂等)", await stub.Admit(
            opb.AdmitRequest(player_id=pid, owner_epoch=1, operation_id=OP1, target=t1)))

        r = await stub.RenewInstanceLease(opb.RenewInstanceLeaseRequest(target=t1, lease_seconds=30))
        print("--- 06 RenewInstanceLease(旧 DS 建租约)")
        print(f"    code={ec.ErrCode.Name(r.code)} lease_deadline_ms={'>0' if r.lease_deadline_ms else '0'}")

        p.dump("07 Begin 迁到新 target(应设 admit_not_before 屏障)", await stub.BeginTransition(
            opb.BeginTransitionRequest(player_id=pid, expect_epoch=1, operation_id=OP2,
                                       owner_type=OWNER_TYPE_HUB, target=t2, source_revision=1 << 24 | 2)))

        # ★ 本次修复的重点:屏障未开时 retry_after_ms 与 record 必须一起回来
        ar = await stub.Admit(opb.AdmitRequest(player_id=pid, owner_epoch=2, operation_id=OP2, target=t2))
        p.dump("08 ★ Admit 屏障未开", ar,
               extra=f" retry_after={bucket(ar.retry_after_ms)} 带记录={ar.HasField('record')}")

        # ★ epoch 冲突必须附当前记录(§9.23 query-first 重查依据)
        br = await stub.BeginTransition(
            opb.BeginTransitionRequest(player_id=pid, expect_epoch=999, operation_id=OP3,
                                       owner_type=OWNER_TYPE_HUB, target=t2, source_revision=1 << 24 | 3))
        p.dump("09 ★ Begin epoch 冲突", br, extra=f" 带记录={br.HasField('record')}")

        p.dump("10 Release(错 epoch)", await stub.ReleaseOwner(
            opb.ReleaseOwnerRequest(player_id=pid, owner_epoch=999, operation_id=OP2)))

        p.dump("11 Release(对 epoch)", await stub.ReleaseOwner(
            opb.ReleaseOwnerRequest(player_id=pid, owner_epoch=2, operation_id=OP2)))

        p.dump("12 Release 重放(幂等)", await stub.ReleaseOwner(
            opb.ReleaseOwnerRequest(player_id=pid, owner_epoch=2, operation_id=OP2)))

        # ── 参数校验 ──
        p.dump("13 Query player_id=0", await stub.QueryOwner(opb.QueryOwnerRequest(player_id=0)))
        p.dump("14 Begin player_id=0", await stub.BeginTransition(
            opb.BeginTransitionRequest(player_id=0, expect_epoch=0, operation_id="op", target=t1)))
        p.dump("15 Begin 空 operation_id", await stub.BeginTransition(
            opb.BeginTransitionRequest(player_id=BASE + 2, expect_epoch=0, operation_id="",
                                       owner_type=OWNER_TYPE_HUB, target=t1)))
        p.dump("16 Admit owner_epoch=0", await stub.Admit(
            opb.AdmitRequest(player_id=BASE + 2, owner_epoch=0, operation_id="op", target=t1)))
        p.dump("17 Renew 空 target", await stub.RenewInstanceLease(
            opb.RenewInstanceLeaseRequest(target=opb.OwnerTarget(), lease_seconds=30)))

        # ── ★ 系统接口守卫:带玩家 JWT 一律拒 ──
        md = (("x-pandora-player-id", "1001"),)
        for name, call in (
            ("QueryOwner", stub.QueryOwner(opb.QueryOwnerRequest(player_id=BASE + 3), metadata=md)),
            ("BeginTransition", stub.BeginTransition(
                opb.BeginTransitionRequest(player_id=BASE + 3, operation_id="x", target=t1), metadata=md)),
            ("Admit", stub.Admit(
                opb.AdmitRequest(player_id=BASE + 3, owner_epoch=1, operation_id="x", target=t1), metadata=md)),
            ("RenewInstanceLease", stub.RenewInstanceLease(
                opb.RenewInstanceLeaseRequest(target=t1, lease_seconds=30), metadata=md)),
            ("ReleaseOwner", stub.ReleaseOwner(
                opb.ReleaseOwnerRequest(player_id=BASE + 3, owner_epoch=1, operation_id="x"), metadata=md)),
        ):
            resp = await call
            print(f"--- 18.{name} 带玩家 JWT")
            print(f"    code={ec.ErrCode.Name(resp.code)}")

        # ══ ★ 19 屏障未开:必须旧 owner = BATTLE 且持活跃租约 ══
        #
        # 旧 owner 是 HUB 时 compute_admit_not_before_ms 刻意返回 now(协作迁移不等待,
        # 否则每次进大厅卡 27 秒),所以用 HUB 永远走不到这条分支 —— 这正是第一版探针
        # "验了个寂寞"的原因:三条 ★ 全落在幂等快路径上,而 diff 仍是零。
        pb = BASE + 10
        tb1 = target("battle-a", "uid-ba", assign="m-1")
        tb2 = target("battle-b", "uid-bb", epoch=2, assign="m-2")
        await stub.BeginTransition(opb.BeginTransitionRequest(
            player_id=pb, expect_epoch=0, operation_id=OP1,
            owner_type=OWNER_TYPE_BATTLE, target=tb1, source_revision=1 << 24 | 1))
        await stub.Admit(opb.AdmitRequest(
            player_id=pb, owner_epoch=1, operation_id=OP1, target=tb1))
        # 旧 BATTLE DS 建 30s 租约 → 屏障 = 租约截止 + 7s 余量,必然在未来
        await stub.RenewInstanceLease(opb.RenewInstanceLeaseRequest(target=tb1, lease_seconds=30))
        p.dump("19a Begin BATTLE→BATTLE(设屏障)", await stub.BeginTransition(
            opb.BeginTransitionRequest(player_id=pb, expect_epoch=1, operation_id=OP2,
                                       owner_type=OWNER_TYPE_BATTLE, target=tb2,
                                       source_revision=1 << 24 | 2)))
        ar2 = await stub.Admit(opb.AdmitRequest(
            player_id=pb, owner_epoch=2, operation_id=OP2, target=tb2))
        p.dump("19b ★ Admit 屏障未开", ar2,
               extra=f" retry_after={bucket(ar2.retry_after_ms)} 带记录={ar2.HasField('record')}")

        # ══ ★ 20 epoch 冲突:必须换一个**不同的** target ══
        #
        # 同 exact target 的重复投递在 epoch 校验**之前**就 no-op 返回了
        # (repo.begin 判定链第 ① 步),所以复用同一 target 永远看不到冲突。
        tb3 = target("battle-c", "uid-bc", epoch=3, assign="m-3")
        br2 = await stub.BeginTransition(opb.BeginTransitionRequest(
            player_id=pb, expect_epoch=999, operation_id=OP3,
            owner_type=OWNER_TYPE_BATTLE, target=tb3, source_revision=1 << 24 | 3))
        p.dump("20 ★ Begin epoch 冲突(换 target)", br2,
               extra=f" 带记录={br2.HasField('record')}")

        # ══ ★ 21 来源版本闸必须排在 no-op 之前 ══
        #
        # 修复前的顺序是「同 target no-op → epoch CAS → 版本闸」,于是**重复投递
        # 整条跳过版本校验**:旧 hub_allocator 拿着更旧的 revision 重投同一 target,
        # 权威照样回 OK。事故形状(INC-20260818-003)正是「旧 binary 握着合法
        # expect_epoch」—— epoch 检查放不倒它,只有版本闸能。
        ph = BASE + 20
        th = target("hub-r", "uid-hr", assign="r-1")
        R2, R1 = (1 << 24) | 2, (1 << 24) | 1
        await stub.BeginTransition(opb.BeginTransitionRequest(
            player_id=ph, expect_epoch=0, operation_id=OP1,
            owner_type=OWNER_TYPE_HUB, target=th, source_revision=R2))
        await stub.Admit(opb.AdmitRequest(
            player_id=ph, owner_epoch=1, operation_id=OP1, target=th))
        sr = await stub.BeginTransition(opb.BeginTransitionRequest(
            player_id=ph, expect_epoch=1, operation_id=OP2,
            owner_type=OWNER_TYPE_HUB, target=th, source_revision=R1))
        print("--- 21 ★ 同 target 重投但 revision 更旧")
        print(f"    code={ec.ErrCode.Name(sr.code)}  ← 必须 STALE,回 OK 说明版本闸排在 no-op 之后")

        # ══ ★ 22 高水位必须在 no-op 早退分支里也推进 ══
        #
        # hub 侧把存量 legacy(0)补成 R 时 target 一个字节都不变,这次 Begin 必然落到
        # no-op 分支;推水位的代码若只在下游就永远走不到,水位永久停在旧值,
        # 「见过非零版本就永久拒 legacy」这条逐玩家防线对这批玩家从不 arm。
        #
        # 判据:同 target 用更高的 R3 重投(走 no-op),再换 target 用 R2 —— 若水位
        # 真的推到了 R3,R2 必须被拒。
        R3 = (1 << 24) | 3
        await stub.BeginTransition(opb.BeginTransitionRequest(
            player_id=ph, expect_epoch=1, operation_id=OP2,
            owner_type=OWNER_TYPE_HUB, target=th, source_revision=R3))
        q = await stub.QueryOwner(opb.QueryOwnerRequest(player_id=ph))
        print("--- 22 ★ no-op 分支后的高水位")
        print(f"    hub_source_revision={q.record.hub_source_revision} 期望={R3}"
              f"  ({'已推进' if q.record.hub_source_revision == R3 else '★ 未推进'})")
        th2 = target("hub-r2", "uid-hr2", epoch=2, assign="r-2")
        sr2 = await stub.BeginTransition(opb.BeginTransitionRequest(
            player_id=ph, expect_epoch=1, operation_id=OP3,
            owner_type=OWNER_TYPE_HUB, target=th2, source_revision=R2))
        print(f"    换 target 用更旧的 R2:code={ec.ErrCode.Name(sr2.code)}  ← 必须 STALE")


asyncio.run(main())
