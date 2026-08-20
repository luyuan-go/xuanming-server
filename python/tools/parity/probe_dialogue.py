import asyncio, sys, re
import pathlib as _pl

# 按**文件位置**解析,不依赖调用时的 cwd —— 探针会被从各种目录调起。
_ROOT = _pl.Path(__file__).resolve().parents[2]   # python/
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "gen"))
import grpc
from google.protobuf import text_format
from pandora.dialogue.v1 import dialogue_pb2 as d, dialogue_pb2_grpc as dg
from pandora.common.v1 import errcode_pb2 as ec

PORT = sys.argv[1]
MD = (("x-pandora-player-id","1001"), ("x-request-id","probe-001"))
OTHER = (("x-pandora-player-id","2002"),)
ids = {}
def norm(t):  # 雪花 ID 归一化成 <ID#n>,其余逐字节保留
    def rep(m):
        v = m.group(1)
        return "dialogue_id: <ID#%d>" % ids.setdefault(v, len(ids)+1)
    return re.sub(r"dialogue_id: (\d{6,})", rep, t)

def dump(tag, resp):
    print(f"--- {tag}")
    print(f"    code_name={ec.ErrCode.Name(resp.code)}")
    body = text_format.MessageToString(resp, as_utf8=True).rstrip()
    print("\n".join("    "+l for l in norm(body).splitlines()) or "    <empty>")

async def main():
    async with grpc.aio.insecure_channel(f"127.0.0.1:{PORT}") as ch:
        st = dg.DialogueServiceStub(ch)
        S = lambda **k: d.StartDialogueRequest(**k)
        r = await st.StartDialogue(S(npc_id=1001), metadata=MD); dump("01 start npc=1001", r)
        did = r.state.dialogue_id
        C = lambda o, i=None, md=MD: st.ChooseOption(d.ChooseOptionRequest(dialogue_id=i or did, option_id=o), metadata=md)
        dump("02 choose '1'", await C("1"))
        dump("03 choose '1' 回环", await C("1"))
        dump("04 choose '3' 终止", await C("3"))
        dump("05 已结束再选", await C("1"))
        dump("06 npc=1002", (r2 := await st.StartDialogue(S(npc_id=1002), metadata=MD)))
        d2 = r2.state.dialogue_id
        dump("07 越权 2002", await C("1", d2, OTHER))
        dump("08 非法 option", await C("99", d2))
        dump("09 空 option", await C("", d2))
        dump("10 end #1", await st.EndDialogue(d.EndDialogueRequest(dialogue_id=d2), metadata=MD))
        dump("11 end #2 幂等", await st.EndDialogue(d.EndDialogueRequest(dialogue_id=d2), metadata=MD))
        dump("12 end 不存在", await st.EndDialogue(d.EndDialogueRequest(dialogue_id=999), metadata=MD))
        dump("13 end 无鉴权", await st.EndDialogue(d.EndDialogueRequest(dialogue_id=999)))
        dump("14 start npc=0", await st.StartDialogue(S(npc_id=0), metadata=MD))
        dump("15 start npc 不存在", await st.StartDialogue(S(npc_id=99999), metadata=MD))
        dump("16 start 无鉴权", await st.StartDialogue(S(npc_id=1001)))
        dump("17 choose 不存在会话", await C("1", 999))
        # 同一玩家对同一 NPC 重复 Start(是否复用会话)
        a = await st.StartDialogue(S(npc_id=1001), metadata=MD)
        b = await st.StartDialogue(S(npc_id=1001), metadata=MD)
        print("--- 18 重复 Start 是否同一会话")
        print("    same_session=%s" % (a.state.dialogue_id == b.state.dialogue_id))
asyncio.run(main())
