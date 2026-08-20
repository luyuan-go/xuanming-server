"""login 服务 Go/Python 对照探针。

用法(两侧起在同一份 conf、只错开端口):

    cd python
    PYTHONUTF8=1 .venv/Scripts/python.exe tools/parity/probe_login.py 20101 91 > /tmp/py.txt 2>&1
    PYTHONUTF8=1 .venv/Scripts/python.exe tools/parity/probe_login.py 20001 92 > /tmp/go.txt 2>&1
    diff /tmp/py.txt /tmp/go.txt

第二个参数是**段号**(1..999),两次运行必须不同。login 的共享资源不止一处:

  - accounts.account 全服唯一(collation 大小写不敏感)—— 账号名必须带段号;
  - player_id / account_id 由 snowflake 铸,两个实现的 node_id 都是 1
    (同一份 yaml),同毫秒同 step 会**逐位重号** —— 所以两侧必须错开时间跑,
    别并行;
  - account_devices 按 (player_id, device_id) 建行,device_id 也带段号;
  - Redis 的会话键 / 票据 jti 键按 player_id 分区,随 player_id 自动隔离。

★ 每一步先断言**前置状态**再断言结果(README ①)。本探针里被这条救回来的两处:
  - 「越权 EnterRole 被拒」必须先证明那个 player_id 确实**不在** B 账号名下,
    否则 ErrLoginRoleNotOwned 可能只是因为 player_id 根本不存在;
  - 「顶号后旧 jti 被拒」必须先证明旧 jti **曾经能用**(step 9 拿到过票),
    否则 ErrSessionSuperseded 可能只是因为那个 jti 从来就没被认过。
"""

from __future__ import annotations

import asyncio
import base64
import json
import pathlib as _pl
import re
import sys

# 按**文件位置**解析,不依赖调用时的 cwd —— 探针会被从各种目录调起。
_ROOT = _pl.Path(__file__).resolve().parents[2]  # python/
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "gen"))

import grpc  # noqa: E402
from google.protobuf import text_format  # noqa: E402
from grpc_health.v1 import health_pb2, health_pb2_grpc  # noqa: E402

from pandora.common.v1 import errcode_pb2 as ec  # noqa: E402
from pandora.login.v1 import login_pb2 as lpb  # noqa: E402
from pandora.login.v1 import login_pb2_grpc as lgrpc  # noqa: E402

PORT = sys.argv[1]
SEG = sys.argv[2]

ACC_A = f"plprobe{SEG}a"
ACC_B = f"plprobe{SEG}b"
DEV_1 = f"pldev{SEG}-1"
DEV_2 = f"pldev{SEG}-2"

# SelectRole 的 role_id 是 CfgRole 配置表 ID(职业外观),不是角色实体。
# dev_allow_any_role=true 时只校非 0,取一个固定值让两侧可 diff。
ROLE_ID = 2

TIMEOUT = 20.0

_FAILS: list[str] = []


# ── 归一化 ────────────────────────────────────────────────────────────────
# 只盖「必然不同」的:snowflake ID(两侧段不同)、绝对时间戳、服务端自生成 UUID、
# 账号名里的段号。**不盖** code、entry_state、route、role_id、ds_type、aud —— 那些
# 正是要验的东西。

_IDS: dict[int, str] = {}


def _pid(v: int) -> str:
    if v == 0:
        return "0"
    return _IDS.setdefault(v, f"<ID{len(_IDS) + 1}>")


_UUID_RE = re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b")


def norm(text: str) -> str:
    text = re.sub(r"\b(\d{15,20})\b", lambda m: _pid(int(m.group(1))), text)
    text = _UUID_RE.sub("<UUID>", text)
    text = text.replace(SEG, "<SEG>")
    return text


def jwt_claims(token: str) -> dict:
    """解出 JWT payload。**不验签** —— 这里要的是 claim 形状,不是有效性。"""
    parts = token.split(".")
    if len(parts) != 3:
        return {}
    raw = parts[1]
    raw += "=" * (-len(raw) % 4)
    try:
        return json.loads(base64.urlsafe_b64decode(raw).decode("utf-8"))
    except Exception:  # noqa: BLE001
        return {}


# 随每次签发必然变化的 claim,只报「在不在」不报值。
_VOLATILE_CLAIMS = {"sub", "iat", "exp", "jti", "sjti", "ds_uid", "ds_credential_jti",
                    "hub_assignment_id", "ds_pod"}


def claim_shape(token: str) -> str:
    """把 token 渲染成可 diff 的 claim 形状。

    ★ 刻意保留 **key 的有序集合 + 非易变 claim 的值**:
    少一个 claim(典型:sjti / role_id / ds_writer_epoch)= DS 侧的一道门失效,
    而 token 本身照样验得过、日志一行不报 —— 只有并排 diff 才看得见。
    JSON 的 key 顺序两侧不同(Go 的 struct tag 序 vs Python 的 dict 序),
    所以按字母排序后再输出。
    """
    if not token:
        return "<empty>"
    c = jwt_claims(token)
    if not c:
        return "<unparseable>"
    out = []
    for k in sorted(c):
        if k in _VOLATILE_CLAIMS:
            out.append(f"{k}=<set>" if c[k] not in ("", 0, None) else f"{k}=<zero>")
        else:
            out.append(f"{k}={json.dumps(c[k], sort_keys=True, ensure_ascii=False)}")
    return "{" + ", ".join(out) + "}"


def dump(tag: str, resp, extra: str = "") -> None:
    print(f"--- {tag}")
    print(f"    code={ec.ErrCode.Name(resp.code)}{extra}")
    body = text_format.MessageToString(resp, as_utf8=True).rstrip()
    lines = []
    for ln in body.splitlines():
        if ln.startswith("code:"):
            continue
        # token 正文太长且必然不同,换成 claim 形状(在下面单独打)
        if re.match(r"\s*(session_token|account_token|hub_ticket|battle_ticket|ticket):", ln):
            continue
        lines.append(ln)
    text = norm("\n".join(lines))
    print("\n".join("    " + ln for ln in text.splitlines()) or "    <empty>")


def show_token(label: str, token: str) -> None:
    print(f"    {label}: {claim_shape(token)}")


def check(cond: bool, what: str) -> None:
    """前置/结果断言。★ 失败不中断 —— 后面的步骤照跑,一次运行拿到全部信息。"""
    mark = "OK " if cond else "FAIL"
    if not cond:
        _FAILS.append(what)
    print(f"    [{mark}] {what}")


def acc_md(account_id: int):
    return (("x-pandora-account-id", str(account_id)),)


def player_md(player_id: int, session_token: str):
    """模拟 Envoy 玩家态注入:player-id + 验签后 payload(base64url 原样转发)。"""
    payload = session_token.split(".")[1] if session_token.count(".") == 2 else ""
    return (
        ("x-pandora-player-id", str(player_id)),
        ("x-pandora-jwt-payload", payload),
    )


async def main() -> None:  # noqa: C901, PLR0915
    async with grpc.aio.insecure_channel(f"127.0.0.1:{PORT}") as ch:
        st = lgrpc.LoginServiceStub(ch)

        # ── 0. 可观测面:grpc health ─────────────────────────────────────
        # 没注册 health 的服务在 k8s 里**永远不会 Ready**,而进程日志全绿。
        print("=== 0. grpc.health.v1.Health")
        hs = health_pb2_grpc.HealthStub(ch)
        r = await hs.Check(health_pb2.HealthCheckRequest(service=""), timeout=TIMEOUT)
        name = health_pb2.HealthCheckResponse.ServingStatus.Name(r.status)
        print(f"    health('') = {name}")
        check(name == "SERVING", "整进程 health 已注册且 SERVING")
        try:
            await hs.Check(
                health_pb2.HealthCheckRequest(service="pandora.login.v1.LoginService"),
                timeout=TIMEOUT,
            )
            print("    health(LoginService) = SERVING")
        except grpc.aio.AioRpcError as e:
            print(f"    health(LoginService) = {e.code().name}")
        print()

        # ── 1. 首登自动注册 + 幂等 ──────────────────────────────────────
        print("=== 1. Login(defer_role_entry=true) 首登自动注册")
        a1 = await st.Login(
            lpb.LoginRequest(account=ACC_A, password_hash="probe-pw", device_id=DEV_1,
                             defer_role_entry=True),
            timeout=TIMEOUT,
        )
        dump("Login(A) #1 defer=true", a1)
        show_token("account_token", a1.account_token)
        check(a1.code == ec.OK, "首登返回 OK(dev_auto_register 生效)")
        check(a1.account_id != 0, "下发了 account_id")
        check(a1.player_id == 0 and a1.session_token == "",
              "defer=true 时**不**下发角色层字段(player_id/session_token 留空)")
        check(len(a1.roles) == 1 and a1.roles[0].role_name == ACC_A,
              "注册时自动建了 1 个角色且角色名=账号名")
        a_pid = a1.roles[0].player_id if a1.roles else 0

        a2 = await st.Login(
            lpb.LoginRequest(account=ACC_A, password_hash="probe-pw", device_id=DEV_1,
                             defer_role_entry=True),
            timeout=TIMEOUT,
        )
        # ★ 这一条证明「真的落库了」:重登拿到**同一个** account_id / player_id,
        # 而不是每次现铸一个 —— 否则整条链可能只是在内存里空转。
        check(a2.account_id == a1.account_id and (a2.roles and a2.roles[0].player_id == a_pid),
              "重复登录复用同一 account_id / player_id(证明写进了 MySQL)")
        print()

        # ── 2. 账号态读路径 ─────────────────────────────────────────────
        print("=== 2. ListAccountRoles(账号态 token 身份)")
        l1 = await st.ListAccountRoles(lpb.ListAccountRolesRequest(),
                                       metadata=acc_md(a1.account_id), timeout=TIMEOUT)
        dump("ListAccountRoles(A)", l1)
        check(l1.code == ec.OK and [x.player_id for x in l1.roles] == [a_pid],
              "列出的正是 A 名下那个角色")

        l0 = await st.ListAccountRoles(lpb.ListAccountRolesRequest(), timeout=TIMEOUT)
        dump("ListAccountRoles(无 account 头)", l0)
        check(l0.code == ec.ERR_UNAUTHORIZED, "缺账号态身份头 → ERR_UNAUTHORIZED(不接受自报)")

        lp = await st.ListAccountRoles(lpb.ListAccountRolesRequest(),
                                       metadata=(("x-pandora-player-id", str(a_pid)),),
                                       timeout=TIMEOUT)
        dump("ListAccountRoles(只带玩家态头)", lp)
        # ★ 玩家态 token 不得当账号态用:回退去读 player-id 等于把两层隔离整个拆掉。
        check(lp.code == ec.ERR_UNAUTHORIZED,
              "玩家态头**不能**冒充账号态(不回退读 x-pandora-player-id)")
        print()

        # ── 3. 越权 EnterRole ───────────────────────────────────────────
        print("=== 3. 越权:拿 B 的账号身份进 A 的角色")
        b1 = await st.Login(
            lpb.LoginRequest(account=ACC_B, password_hash="probe-pw", device_id=DEV_2,
                             defer_role_entry=True),
            timeout=TIMEOUT,
        )
        check(b1.code == ec.OK and b1.account_id != 0, "B 账号注册成功")
        check(b1.account_id != a1.account_id, "A / B 是两个不同的 account_id")
        b_pids = [x.player_id for x in b1.roles]
        # ★ 前置断言(README ①):必须先证明 a_pid **不在** B 名下 ——
        # 否则下面的拒绝可能只是因为「这个角色根本不存在」,验了个寂寞。
        check(a_pid not in b_pids and a_pid != 0,
              f"前置:A 的角色不在 B 的角色列表里(B 有 {len(b_pids)} 个角色)")

        e_bad = await st.EnterRole(
            lpb.EnterRoleRequest(player_id=a_pid, device_id=DEV_2),
            metadata=acc_md(b1.account_id), timeout=TIMEOUT,
        )
        dump("EnterRole(B 的身份 + A 的 player_id)", e_bad)
        check(e_bad.code == ec.ERR_LOGIN_ROLE_NOT_OWNED,
              "越权进角色被 account_roles 台账拦下 → ERR_LOGIN_ROLE_NOT_OWNED")

        e_zero = await st.EnterRole(
            lpb.EnterRoleRequest(player_id=0, device_id=DEV_1),
            metadata=acc_md(a1.account_id), timeout=TIMEOUT,
        )
        dump("EnterRole(player_id=0)", e_zero)

        e_noauth = await st.EnterRole(
            lpb.EnterRoleRequest(player_id=a_pid, device_id=DEV_1), timeout=TIMEOUT,
        )
        dump("EnterRole(无账号头)", e_noauth)
        check(e_noauth.code == ec.ERR_UNAUTHORIZED, "缺账号态身份 → ERR_UNAUTHORIZED")
        print()

        # ── 4. 正常进角色 ───────────────────────────────────────────────
        print("=== 4. EnterRole(本人角色)")
        e1 = await st.EnterRole(
            lpb.EnterRoleRequest(player_id=a_pid, device_id=DEV_1),
            metadata=acc_md(a1.account_id), timeout=TIMEOUT,
        )
        dump("EnterRole(A)", e1)
        show_token("session_token", e1.session_token)
        check(e1.code == ec.OK and e1.session_token != "", "拿到玩家态 session_token")
        check(e1.player_id == a_pid, "session 绑的是选中的那个角色")
        # ★ 前置断言:此刻**还没选过职业**,所以进场态必须是 ROLE_REQUIRED。
        # 少了这一条,下面「选完职业后变 TARGET」就证明不了是 SelectRole 起的作用。
        check(e1.selected_role_id == 0, "前置:该角色尚未选过职业(selected_role_id=0)")
        check(e1.resume_context.entry_state == lpb.RESUME_ENTRY_STATE_ROLE_REQUIRED,
              "未选职业 → entry_state=ROLE_REQUIRED(不冒充可进场)")
        check(e1.hub_ticket == "", "未选职业时不签 hub 票")
        s1 = e1.session_token
        print()

        # ── 5. SelectRole ───────────────────────────────────────────────
        print("=== 5. SelectRole(选职业外观)")
        s_noauth = await st.SelectRole(lpb.SelectRoleRequest(role_id=ROLE_ID), timeout=TIMEOUT)
        dump("SelectRole(无玩家头)", s_noauth)
        check(s_noauth.code == ec.ERR_UNAUTHORIZED, "缺玩家态身份 → ERR_UNAUTHORIZED")

        s_zero = await st.SelectRole(lpb.SelectRoleRequest(role_id=0),
                                     metadata=player_md(a_pid, s1), timeout=TIMEOUT)
        dump("SelectRole(role_id=0)", s_zero)

        s_ok = await st.SelectRole(lpb.SelectRoleRequest(role_id=ROLE_ID),
                                   metadata=player_md(a_pid, s1), timeout=TIMEOUT)
        dump("SelectRole(role_id=2)", s_ok)
        show_token("hub_ticket", s_ok.hub_ticket)
        check(s_ok.code == ec.OK and s_ok.hub_ds_addr != "", "选职业成功并拿到 hub 地址")
        check(s_ok.hub_ticket != "", "签出了 hub 票据")
        hub_ticket = s_ok.hub_ticket
        hub_claims = jwt_claims(hub_ticket)
        check(hub_claims.get("role_id") == ROLE_ID, "hub 票里带的是刚选的 role_id")

        # ★ 后置断言:重新进角色应看到职业已落库、进场态从 ROLE_REQUIRED 翻成 TARGET。
        e2 = await st.EnterRole(
            lpb.EnterRoleRequest(player_id=a_pid, device_id=DEV_1),
            metadata=acc_md(a1.account_id), timeout=TIMEOUT,
        )
        dump("EnterRole(A) 选完职业后", e2)
        check(e2.selected_role_id == ROLE_ID, "职业已落 player_roles(重进可见)")
        check(e2.resume_context.entry_state != lpb.RESUME_ENTRY_STATE_ROLE_REQUIRED,
              "选完职业后 entry_state 不再是 ROLE_REQUIRED")
        s2 = e2.session_token
        print()

        # ── 6. 会话现行性门(顶号) ──────────────────────────────────────
        print("=== 6. 顶号:旧会话的 jti 必须失效")
        # ★ 前置断言在 step 5 已给出:s1 的 jti **曾经**能签出 hub 票(s_ok.code==OK)。
        # 没有这一条,下面的 SESSION_SUPERSEDED 可能只是「这个 jti 从来没被认过」。
        check(s_ok.code == ec.OK, "前置:旧会话 s1 之前确实能 SelectRole 成功")
        check(s1 != s2 and jwt_claims(s1).get("jti") != jwt_claims(s2).get("jti"),
              "前置:第二次 EnterRole 铸了新会话(jti 已轮换)")
        s_stale = await st.SelectRole(lpb.SelectRoleRequest(role_id=ROLE_ID),
                                      metadata=player_md(a_pid, s1), timeout=TIMEOUT)
        dump("SelectRole(旧会话 s1)", s_stale)
        check(s_stale.code == ec.ERR_SESSION_SUPERSEDED,
              "被顶设备 SelectRole → ERR_SESSION_SUPERSEDED(不是 UNAUTHORIZED)")
        s_fresh = await st.SelectRole(lpb.SelectRoleRequest(role_id=ROLE_ID),
                                      metadata=player_md(a_pid, s2), timeout=TIMEOUT)
        dump("SelectRole(新会话 s2)", s_fresh)
        check(s_fresh.code == ec.OK, "新会话照常可用(证明拒的是代际而不是整条路径坏了)")
        print()

        # ── 7. IssueDSTicket ────────────────────────────────────────────
        print("=== 7. IssueDSTicket(ds_type=hub)")
        i_noauth = await st.IssueDSTicket(
            lpb.IssueDSTicketRequest(session_token=s2, ds_type="hub"), timeout=TIMEOUT)
        dump("IssueDSTicket(无玩家头)", i_noauth)
        check(i_noauth.code == ec.ERR_UNAUTHORIZED, "缺玩家态身份 → ERR_UNAUTHORIZED")

        # ★ 顺序探针:旧 token 必须先撞会话门,而不是先撞「未实现」。
        # 两侧同为会话错误 = 门的**相对顺序**一致;Python 若先返回 NOT_IMPLEMENTED,
        # 说明它把会话门排在了后面 —— 那是一道被绕开的安全门。
        i_stale = await st.IssueDSTicket(
            lpb.IssueDSTicketRequest(session_token=s1, ds_type="hub"),
            metadata=player_md(a_pid, s1), timeout=TIMEOUT)
        dump("IssueDSTicket(旧会话 s1)", i_stale)
        check(i_stale.code == ec.ERR_SESSION_SUPERSEDED,
              "旧会话先撞会话门(门序与 Go 一致)")

        i_ok = await st.IssueDSTicket(
            lpb.IssueDSTicketRequest(session_token=s2, ds_type="hub"),
            metadata=player_md(a_pid, s2), timeout=TIMEOUT)
        dump("IssueDSTicket(hub, 新会话)", i_ok, extra=f" hub_ds_addr={'set' if i_ok.hub_ds_addr else 'empty'}")
        show_token("ticket", i_ok.ticket)

        i_bat = await st.IssueDSTicket(
            lpb.IssueDSTicketRequest(session_token=s2, ds_type="battle", target_id=1),
            metadata=player_md(a_pid, s2), timeout=TIMEOUT)
        dump("IssueDSTicket(battle, 不存在的 match)", i_bat)

        i_junk = await st.IssueDSTicket(
            lpb.IssueDSTicketRequest(session_token=s2, ds_type="nonsense"),
            metadata=player_md(a_pid, s2), timeout=TIMEOUT)
        dump("IssueDSTicket(ds_type 非法)", i_junk)
        print()

        # ── 8. VerifyDSTicket ───────────────────────────────────────────
        print("=== 8. VerifyDSTicket(兑换点 + 防重放)")
        pod = hub_claims.get("ds_pod", "probe-pod")
        v1 = await st.VerifyDSTicket(
            lpb.VerifyDSTicketRequest(ticket=hub_ticket, ds_pod_name=pod), timeout=TIMEOUT)
        dump("VerifyDSTicket(hub 票首兑)", v1)
        # ★ 前置断言:这张票是 step 5 刚签出来的,且带 role_id —— 首兑必须成功,
        # 否则下面的「重放被拒」可能只是因为票本来就无效。
        check(v1.code == ec.OK, "前置:合法 hub 票首次兑换成功")
        check(v1.claims.role_id == ROLE_ID and v1.claims.ds_type == "hub",
              "兑换回来的 claims 与签发时一致")

        v2 = await st.VerifyDSTicket(
            lpb.VerifyDSTicketRequest(ticket=hub_ticket, ds_pod_name=pod), timeout=TIMEOUT)
        dump("VerifyDSTicket(同票重放)", v2)
        check(v2.code == ec.ERR_LOGIN_TICKET_REPLAYED,
              "同一张票第二次兑换被拒(jti 已消费,证明 Redis jti 仓储在工作)")

        v3 = await st.VerifyDSTicket(
            lpb.VerifyDSTicketRequest(ticket="not.a.jwt", ds_pod_name=pod), timeout=TIMEOUT)
        dump("VerifyDSTicket(垃圾票)", v3)

        v4 = await st.VerifyDSTicket(
            lpb.VerifyDSTicketRequest(ticket="", ds_pod_name=pod), timeout=TIMEOUT)
        dump("VerifyDSTicket(空票)", v4)

        # 换个 pod 名兑另一张票:pod 绑定是否被核对
        s_ok2 = await st.SelectRole(lpb.SelectRoleRequest(role_id=ROLE_ID),
                                    metadata=player_md(a_pid, s2), timeout=TIMEOUT)
        check(s_ok2.code == ec.OK, "前置:又签了一张新 hub 票用于 pod 绑定探针")
        v5 = await st.VerifyDSTicket(
            lpb.VerifyDSTicketRequest(ticket=s_ok2.hub_ticket, ds_pod_name="wrong-pod-name"),
            timeout=TIMEOUT)
        dump("VerifyDSTicket(pod 名不符)", v5)
        print()

        # ── 9. 编号 / resume / 登出 ─────────────────────────────────────
        print("=== 9. GetPlayerNo / GetRegisterNo / GetResumeContext / Logout")
        n_noauth = await st.GetPlayerNo(lpb.GetPlayerNoRequest(), timeout=TIMEOUT)
        dump("GetPlayerNo(无玩家头)", n_noauth)
        check(n_noauth.code == ec.ERR_UNAUTHORIZED, "缺玩家态身份 → ERR_UNAUTHORIZED")

        n1 = await st.GetPlayerNo(lpb.GetPlayerNoRequest(),
                                  metadata=player_md(a_pid, s2), timeout=TIMEOUT)
        dump("GetPlayerNo", n1, extra=f" player_no={'0(补号窗口内)' if n1.player_no == 0 else 'assigned'}")
        n2 = await st.GetRegisterNo(lpb.GetRegisterNoRequest(),
                                    metadata=player_md(a_pid, s2), timeout=TIMEOUT)
        dump("GetRegisterNo(旧客户端兼容入口)", n2,
             extra=f" register_no={'0' if n2.register_no == 0 else 'assigned'}")
        check(n2.code == n1.code and n2.register_no == n1.player_no,
              "兼容入口与 GetPlayerNo 逐字段同值(两条路径不会分叉)")

        rc1 = await st.GetResumeContext(lpb.GetResumeContextRequest(session_token=s2),
                                        timeout=TIMEOUT)
        dump("GetResumeContext(有效 token)", rc1)
        check(rc1.code == ec.OK, "有效会话能取回权威路由")

        rc_bad = await st.GetResumeContext(lpb.GetResumeContextRequest(session_token="junk"),
                                           timeout=TIMEOUT)
        dump("GetResumeContext(垃圾 token)", rc_bad)

        rc_stale = await st.GetResumeContext(lpb.GetResumeContextRequest(session_token=s1),
                                             timeout=TIMEOUT)
        dump("GetResumeContext(旧会话 s1)", rc_stale)

        lo1 = await st.Logout(lpb.LogoutRequest(session_token=s2), timeout=TIMEOUT)
        dump("Logout(s2)", lo1)
        check(lo1.code == ec.OK, "登出成功")

        lo2 = await st.Logout(lpb.LogoutRequest(session_token=s2), timeout=TIMEOUT)
        dump("Logout(重复登出)", lo2)

        after = await st.SelectRole(lpb.SelectRoleRequest(role_id=ROLE_ID),
                                    metadata=player_md(a_pid, s2), timeout=TIMEOUT)
        dump("SelectRole(登出后用同一 token)", after)
        # ★ 前置断言:s2 在登出**之前**是能用的(step 6 的 s_fresh 就是它)。
        check(s_fresh.code == ec.OK, "前置:s2 登出前确实可用")
        check(after.code != ec.OK, "登出后旧 token 不再能签票")

        lo_junk = await st.Logout(lpb.LogoutRequest(session_token="junk"), timeout=TIMEOUT)
        dump("Logout(垃圾 token)", lo_junk)
        print()

        # ── 10. 登录参数边界 ────────────────────────────────────────────
        print("=== 10. Login 参数边界")
        for tag, req in (
            ("空账号", lpb.LoginRequest(account="", password_hash="x", device_id=DEV_1)),
            ("空设备", lpb.LoginRequest(account=ACC_A, password_hash="x", device_id="")),
            ("空密码", lpb.LoginRequest(account=ACC_A, password_hash="", device_id=DEV_1)),
            ("超长账号", lpb.LoginRequest(account="z" * 200, password_hash="x", device_id=DEV_1)),
            ("大小写变体", lpb.LoginRequest(account=ACC_A.upper(), password_hash="x",
                                            device_id=DEV_1, defer_role_entry=True)),
        ):
            resp = await st.Login(req, timeout=TIMEOUT)
            dump(f"Login({tag})", resp)
        print()

    print("=== 汇总")
    if _FAILS:
        print(f"    断言失败 {len(_FAILS)} 条:")
        for f in _FAILS:
            print(f"      - {f}")
    else:
        print("    全部断言通过")


asyncio.run(main())
