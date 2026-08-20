"""guild 的 conf / ds_guard / biz / service 层测试(不需要数据库)。

咬住的都是"改了不报错"的那一类:

  ① conf 默认值与 Go 的 `Defaults()` 逐字段一致,**连判据符号都要一样**
     —— rate_quota_per_min 是全服唯一一个 `== 0`(负值 = 关闭)。
  ② service 层的身份必须取自鉴权上下文(R5),请求体里的 player_id 一律无视。
  ③ 业务失败返回 in-band code + gRPC status OK,**不是** abort。
  ④ GetPlayerGuild 的两道门:systemOnly + DS 回调令牌守卫。
  ⑤ 缓存 / kafka 是弱依赖:坏掉只降级,绝不把错误抛给调用方。
  ⑥ 推送原则 2:通知不回发操作者本人;解散是例外(全员,含会长)。
"""

from __future__ import annotations

import asyncio
import pathlib
import re
import time

# ★ 这行不是摆设,它**就是**"grpc 依赖可用"的守卫本体:缺 grpc 时 pytest 在
# **collect 阶段**直接把整个文件 ERROR 掉(实测 1 error / 0 passed / 0 failed),
# 本文件所有用例一条都不会跑 —— 骗不了人。
#
# 这里原先还有一条 `test_grpc_module_is_importable`(`assert grpc is not None`)
# 号称守这件事,已删:成功 import 的模块永远不是 None,那两句断言**结构上无法变红**;
# 而真的缺 grpc 时它连执行机会都没有(见上)。承诺是假的,保障来自这一行 import。
#
# 也别改成 `pytest.importorskip`:grpc 是**硬依赖**(被测的
# pandorapy/services/guild/service.py 自己就无条件 `import grpc`),
# importorskip 会把响亮的 collect ERROR 降级成静默 skip —— CI 里一片 "skipped"
# 比一个 error 更容易被当成绿灯放过去,那是把守卫改弱不是改强。
import jwt as pyjwt
import pytest
from pandora.common.v1 import errcode_pb2
from pandora.group.v1 import group_pb2
from pandora.guild.v1 import guild_pb2

from pandorapy import errcode
from pandorapy.services.guild import biz as gbiz
from pandorapy.services.guild import conf as gconf
from pandorapy.services.guild import ds_guard as gds
from pandorapy.services.guild import rows as grows
from pandorapy.services.guild import service as gsvc

GUILD_DEV_YAML = "../services/social/guild/etc/guild-dev.yaml"


# ══ 夹具:假 context / 假 repo / 假 pusher ═══════════════════════════════════


class FakeContext:
    """最小 gRPC ServicerContext 替身:只需要 invocation_metadata()。"""

    def __init__(self, **headers: str) -> None:
        self._md = tuple((k.replace("_", "-"), v) for k, v in headers.items())

    def invocation_metadata(self):  # noqa: ANN201
        return self._md


def ctx_player(player_id: int) -> FakeContext:
    """带 Envoy 注入的玩家身份头。"""
    return FakeContext(**{"x-pandora-player-id": str(player_id)})


class FakeSnowflake:
    def __init__(self, start: int = 1) -> None:
        self._n = start

    def generate(self) -> int:
        self._n += 1
        return self._n


class FakeGuildRepo:
    """内存版公会 repo。只实现 biz 用到的方法。"""

    def __init__(self) -> None:
        self.guilds: dict[int, grows.GuildRow] = {}
        self.members: dict[int, grows.GuildMemberRow] = {}
        self.requests: dict[int, grows.GuildJoinRequestRow] = {}
        self.swept: list[tuple] = []

    async def create_guild(self, gid, leader_id, name, max_members):  # noqa: ANN001
        self.guilds[gid] = grows.GuildRow(gid, name, leader_id, 1, max_members, 0)
        self.members[leader_id] = grows.GuildMemberRow(
            leader_id, gid, grows.GUILD_ROLE_LEADER, 0
        )

    async def get_guild(self, gid):  # noqa: ANN001
        return self.guilds.get(gid)

    async def get_my_guild(self, pid):  # noqa: ANN001
        m = self.members.get(pid)
        return self.guilds.get(m.guild_id) if m else None

    async def get_member(self, pid):  # noqa: ANN001
        return self.members.get(pid)

    async def list_members(self, gid, cursor=0, limit=0):  # noqa: ANN001
        rows = sorted(
            (m for m in self.members.values() if m.guild_id == gid),
            key=lambda m: m.player_id,
        )
        rows = [m for m in rows if cursor == 0 or m.player_id > cursor]
        return rows[:limit] if limit > 0 else rows

    async def create_join_request(self, rid, gid, pid, max_pending):  # noqa: ANN001
        self.requests[rid] = grows.GuildJoinRequestRow(
            rid, gid, pid, grows.JOIN_STATUS_PENDING, 0
        )
        return rid, False

    async def get_request(self, rid):  # noqa: ANN001
        return self.requests.get(rid)

    async def list_pending_requests(self, gid, cursor=0, limit=0):  # noqa: ANN001
        rows = sorted(
            (r for r in self.requests.values() if r.guild_id == gid), key=lambda r: r.request_id
        )
        rows = [r for r in rows if cursor == 0 or r.request_id > cursor]
        return rows[:limit] if limit > 0 else rows

    async def approve_join(self, rid, approver_id, max_members):  # noqa: ANN001
        rq = self.requests[rid]
        rq.status = grows.JOIN_STATUS_APPROVED
        self.members[rq.player_id] = grows.GuildMemberRow(
            rq.player_id, rq.guild_id, grows.GUILD_ROLE_MEMBER, 0
        )
        self.guilds[rq.guild_id].member_count += 1
        return True

    async def reject_join(self, rid, approver_id):  # noqa: ANN001
        self.requests[rid].status = grows.JOIN_STATUS_REJECTED
        return True

    async def remove_member(self, gid, pid):  # noqa: ANN001
        self.members.pop(pid, None)

    async def kick_member(self, gid, operator_id, target_id):  # noqa: ANN001
        self.members.pop(target_id, None)

    async def disband_guild(self, gid, operator_id):  # noqa: ANN001
        deleted = [p for p, m in self.members.items() if m.guild_id == gid]
        for p in deleted:
            self.members.pop(p)
        self.guilds.pop(gid, None)
        return deleted

    async def set_role(self, gid, operator_id, target_id, role):  # noqa: ANN001
        self.members[target_id].role = role

    async def transfer_leader(self, gid, old_id, new_id):  # noqa: ANN001
        self.members[old_id].role = grows.GUILD_ROLE_MEMBER
        self.members[new_id].role = grows.GUILD_ROLE_LEADER
        self.guilds[gid].leader_id = new_id

    async def sweep_terminal_join_requests(self, mode, days, limit):  # noqa: ANN001
        from pandorapy import dbguard

        self.swept.append((mode, days, limit))
        return dbguard.Outcome(mode=mode, matched=0, deleted=0)


class RecordingPusher:
    def __init__(self) -> None:
        self.sent: list[tuple[int, int]] = []  # (to_player_id, event_type)

    async def push_guild_event(self, to_player_id: int, evt) -> None:  # noqa: ANN001
        self.sent.append((to_player_id, int(evt.type)))
        assert evt.to_player_id == to_player_id, "事件里的 to_player_id 必须与 key 一致"


class ExplodingPusher:
    async def push_guild_event(self, to_player_id: int, evt) -> None:  # noqa: ANN001
        raise RuntimeError("kafka down")


class ExplodingCache:
    """每个方法都炸 —— 用来验证「缓存是弱依赖」这条不是嘴上说说。"""

    async def get_guild(self, gid):  # noqa: ANN001
        raise RuntimeError("redis down")

    async def set_guild(self, g, ttl):  # noqa: ANN001
        raise RuntimeError("redis down")

    async def del_guild(self, gid):  # noqa: ANN001
        raise RuntimeError("redis down")

    async def get_member_guild_id(self, pid):  # noqa: ANN001
        raise RuntimeError("redis down")

    async def set_member_guild_id(self, pid, gid, ttl):  # noqa: ANN001
        raise RuntimeError("redis down")

    async def del_member(self, pid):  # noqa: ANN001
        raise RuntimeError("redis down")


def make_cfg(**overrides) -> gconf.GuildConf:
    cfg = gconf.Config()
    cfg.apply_defaults()
    for k, v in overrides.items():
        setattr(cfg.guild, k, v)
    return cfg.guild


# ══ ① conf 默认值 ═══════════════════════════════════════════════════════════


def test_defaults_match_go(repo_root: pathlib.Path) -> None:
    """★ 逐字段对着 Go 源码断言,而不是对着自己的常量。

    对自己的常量断言只能证明"我没改过自己";这里读的是 Go 的 conf.go,
    Go 侧一改而 Python 没跟,这条就红。
    """
    src = (repo_root / "services/social/guild/internal/conf/conf.go").read_text(
        encoding="utf-8"
    )
    expected = {
        "MaxGuildMembers": (gconf.DEFAULT_MAX_GUILD_MEMBERS, "<="),
        "MaxGroupMembers": (gconf.DEFAULT_MAX_GROUP_MEMBERS, "<="),
        "RateQuotaPerMin": (gconf.DEFAULT_RATE_QUOTA_PER_MIN, "=="),
        "MaxPendingRequestsPerGuild": (gconf.DEFAULT_MAX_PENDING_REQUESTS_PER_GUILD, "<="),
        "MaxGroupsPerPlayer": (gconf.DEFAULT_MAX_GROUPS_PER_PLAYER, "<="),
        "MaxNameLen": (gconf.DEFAULT_MAX_NAME_LEN, "<="),
        "RequestRetentionDays": (gconf.DEFAULT_REQUEST_RETENTION_DAYS, "<="),
        "SweepBatch": (gconf.DEFAULT_SWEEP_BATCH, "<="),
    }
    for field, (value, op) in expected.items():
        pattern = rf"c\.Guild\.{field}\s*{re.escape(op)}\s*0\s*\{{\s*\n\s*c\.Guild\.{field}\s*=\s*(\d+)"
        m = re.search(pattern, src)
        assert m, f"Go conf.go 里找不到 {field} 的 `{op} 0` 判据(判据符号可能被改了)"
        assert int(m.group(1)) == value, f"{field} 默认值分叉:Go={m.group(1)} Python={value}"
    assert 'c.Server.Grpc.Addr = ":20008"' in src
    assert 'c.Server.Http.Addr = ":21008"' in src


def test_negative_rate_quota_means_disabled_not_defaulted() -> None:
    """★ rate_quota_per_min 是唯一用 `== 0` 的字段:**负值 = 显式关闭**。

    抄成 `<= 0` 的话,运维写 -1 想关掉配额,却被静默兜成 10/分钟 —— 两边都不报错。
    """
    cfg = gconf.Config.model_validate({"guild": {"rate_quota_per_min": -1}})
    cfg.apply_defaults()
    assert cfg.guild.rate_quota_per_min == -1
    # 而其余字段的负值仍按"没配"兜默认(Go 用 `<= 0`)
    cfg2 = gconf.Config.model_validate({"guild": {"sweep_batch": -5, "max_name_len": -1}})
    cfg2.apply_defaults()
    assert cfg2.guild.sweep_batch == gconf.DEFAULT_SWEEP_BATCH
    assert cfg2.guild.max_name_len == gconf.DEFAULT_MAX_NAME_LEN


def test_dev_yaml_loads_and_session_gate_is_modeled() -> None:
    """★ session_gate / kafka / ds_auth 必须是**建模字段**,不能落进 model_extra。

    落进 extra 的字段"配了却不生效且零信号" —— prod 的 session_gate.require=true
    会静默退化成 dev 宽松档。
    """
    cfg = gconf.Config.load(GUILD_DEV_YAML)
    assert cfg.server.grpc.addr == ":20008"
    assert cfg.server.http.addr == ":21008"
    assert "session_gate" not in (cfg.model_extra or {})
    assert "kafka" not in (cfg.model_extra or {})
    assert "ds_auth" not in (cfg.model_extra or {})
    assert cfg.kafka.brokers == ["127.0.0.1:9093"]
    assert cfg.ds_auth.mode == "permissive"
    # DSAuth.Defaults() 必须已经填过
    assert cfg.ds_auth.issuer == gconf.DEFAULT_DS_AUTH_ISSUER
    assert cfg.ds_auth.audience == gconf.DEFAULT_DS_AUTH_AUDIENCE
    assert cfg.ds_auth.authority_mode == gconf.DEFAULT_DS_AUTH_AUTHORITY_MODE


@pytest.mark.parametrize("bad", ["delet", "Delete me", "report only", "true"])
def test_retention_mode_typo_is_fail_fast(bad: str) -> None:
    """★ 拼错必须拒启,不能静默回落 report_only(运维以为开了清理、实际一行没删)。"""
    cfg = gconf.Config.model_validate({"guild": {"retention_mode": bad}})
    cfg.apply_defaults()
    with pytest.raises(ValueError):
        cfg.guild.validate_retention_mode()


@pytest.mark.parametrize(
    ("raw", "expect"), [("", "report_only"), ("report_only", "report_only"), ("delete", "delete")]
)
def test_retention_mode_parsed(raw: str, expect: str) -> None:
    cfg = gconf.Config.model_validate({"guild": {"retention_mode": raw}})
    cfg.apply_defaults()
    assert cfg.guild.retention_mode_parsed().value == expect


# ══ ② 分页钳制 ═════════════════════════════════════════════════════════════


@pytest.mark.parametrize(("given", "want"), [(0, 50), (-1, 50), (10, 10), (100, 100), (999, 100)])
def test_clamp_limit(given: int, want: int) -> None:
    assert gbiz.clamp_limit(given) == want


# ══ ③ DS 回调令牌守卫 ═══════════════════════════════════════════════════════


SECRET = "pandora-dev-jwt-secret-change-me-32!"


def make_ds_token(
    *, ds_type: str = "hub", pod: str = "hub-0", match_id: int = 0, exp_delta: int = 600
) -> str:
    now = int(time.time())
    claims = {
        "iss": gconf.DEFAULT_DS_AUTH_ISSUER,
        "sub": pod,
        "aud": [gconf.DEFAULT_DS_AUTH_AUDIENCE],
        "iat": now,
        "exp": now + exp_delta,
        "ds_type": ds_type,
    }
    if match_id:
        claims["match_id"] = match_id
    return pyjwt.encode(
        claims, SECRET, algorithm="HS256", headers={"kid": gds.key_fingerprint(SECRET)}
    )


def make_guard(mode: str) -> gds.DSCallbackGuard | None:
    ds = gconf.DSAuthConf(mode=mode, secret=SECRET)
    ds.apply_defaults()
    return gds.new_from_conf(ds)


def test_guard_off_returns_none() -> None:
    assert make_guard("") is None
    assert make_guard("off") is None


def test_guard_requires_secret() -> None:
    """★ mode!=off 却没配 secret 是**配置矛盾**,必须启动即报错,不能静默不校验。"""
    ds = gconf.DSAuthConf(mode="enforce")
    ds.apply_defaults()
    with pytest.raises(ValueError, match="requires ds_auth.secret"):
        gds.new_from_conf(ds)


def test_guard_rejects_bad_mode() -> None:
    ds = gconf.DSAuthConf(mode="enfroce", secret=SECRET)
    ds.apply_defaults()
    with pytest.raises(ValueError, match="ds_auth.mode invalid"):
        gds.new_from_conf(ds)


def test_guard_rejects_empty_additional_secret() -> None:
    """轮换清单里留了空占位 = 旧密钥其实已断档,必须 fail-closed。"""
    ds = gconf.DSAuthConf(mode="enforce", secret=SECRET, additional_secrets=[""])
    ds.apply_defaults()
    with pytest.raises(ValueError, match="additional_secrets"):
        gds.new_from_conf(ds)


def test_enforce_rejects_missing_token_on_require_token_scope() -> None:
    """★ 纯 DS 回调:没有合法的东西向无令牌调用者,无令牌即拒。"""
    guard = make_guard("enforce")
    with pytest.raises(errcode.PandoraError) as exc:
        guard.check(FakeContext(), gds.DSScope(require_token=True))
    assert exc.value.code == errcode.ErrUnauthorized


def test_permissive_allows_missing_token() -> None:
    """★ permissive 是**观察期**:降级放行,但必须留下告警日志(见 _reject)。"""
    guard = make_guard("permissive")
    guard.check(FakeContext(), gds.DSScope(require_token=True))  # 不抛


def test_enforce_accepts_valid_hub_token() -> None:
    guard = make_guard("enforce")
    ctx = FakeContext(authorization=f"Bearer {make_ds_token()}")
    claims = guard.check_with_claims(ctx, gds.DSScope(require_token=True))
    assert claims is not None and claims["ds_type"] == "hub"


def test_enforce_rejects_wrong_secret() -> None:
    guard = make_guard("enforce")
    other = pyjwt.encode(
        {
            "iss": gconf.DEFAULT_DS_AUTH_ISSUER,
            "sub": "hub-0",
            "aud": [gconf.DEFAULT_DS_AUTH_AUDIENCE],
            "exp": int(time.time()) + 600,
            "ds_type": "hub",
        },
        "another-secret-that-is-long-enough-32",
        algorithm="HS256",
    )
    ctx = FakeContext(authorization=f"Bearer {other}")
    with pytest.raises(errcode.PandoraError) as exc:
        guard.check(ctx, gds.DSScope(require_token=True))
    assert exc.value.code == errcode.ErrUnauthorized


def test_enforce_rejects_expired_token() -> None:
    guard = make_guard("enforce")
    ctx = FakeContext(authorization=f"Bearer {make_ds_token(exp_delta=-10)}")
    with pytest.raises(errcode.PandoraError):
        guard.check(ctx, gds.DSScope(require_token=True))


def test_enforce_rejects_token_without_exp() -> None:
    """★ 没有 exp 的令牌 = 永久凭证,必须拒。"""
    guard = make_guard("enforce")
    tok = pyjwt.encode(
        {
            "iss": gconf.DEFAULT_DS_AUTH_ISSUER,
            "sub": "hub-0",
            "aud": [gconf.DEFAULT_DS_AUTH_AUDIENCE],
            "ds_type": "hub",
        },
        SECRET,
        algorithm="HS256",
    )
    with pytest.raises(errcode.PandoraError):
        guard.check(FakeContext(authorization=f"Bearer {tok}"), gds.DSScope(require_token=True))


def test_battle_token_without_match_id_is_rejected() -> None:
    guard = make_guard("enforce")
    ctx = FakeContext(authorization=f"Bearer {make_ds_token(ds_type='battle', pod='')}")
    with pytest.raises(errcode.PandoraError):
        guard.check(ctx, gds.DSScope(require_token=True))


def test_east_west_call_without_token_passes_when_not_require_token() -> None:
    """内部东西向调用(无网关标记、无令牌)不受守卫影响。"""
    guard = make_guard("enforce")
    guard.check(FakeContext(), gds.DSScope())  # 不抛


def test_gateway_marked_call_without_token_is_rejected() -> None:
    guard = make_guard("enforce")
    ctx = FakeContext(**{"x-pandora-ds-gateway": "1"})
    with pytest.raises(errcode.PandoraError) as exc:
        guard.check(ctx, gds.DSScope())
    assert exc.value.code == errcode.ErrUnauthorized


def test_pod_scope_mismatch_is_permission_deny() -> None:
    guard = make_guard("enforce")
    ctx = FakeContext(authorization=f"Bearer {make_ds_token(pod='hub-0')}")
    with pytest.raises(errcode.PandoraError) as exc:
        guard.check(ctx, gds.DSScope(require_token=True, pod="hub-9"))
    assert exc.value.code == errcode.ErrPermissionDeny


# ══ ④ service 层:R5 身份 + in-band code ═══════════════════════════════════


@pytest.fixture
def guild_stack():
    repo = FakeGuildRepo()
    pusher = RecordingPusher()
    uc = gbiz.GuildUsecase(repo, None, pusher, make_cfg())
    svc = gsvc.GuildService(uc, FakeSnowflake(100), FakeSnowflake(200))
    return repo, pusher, uc, svc


async def test_unauthenticated_write_returns_inband_unauthorized(guild_stack) -> None:
    """★ 业务失败是 in-band code + gRPC status OK,不是 abort。"""
    _repo, _pusher, _uc, svc = guild_stack
    resp = await svc.CreateGuild(guild_pb2.CreateGuildRequest(name="A"), FakeContext())
    assert resp.code == errcode_pb2.ERR_UNAUTHORIZED
    assert resp.guild_id == 0


async def test_create_guild_uses_context_identity(guild_stack) -> None:
    """★ R5:身份取鉴权上下文。请求体里没有 player_id 字段可填,这里验的是
    「确实用了 ctx 里的那个人」—— 建出来的公会会长必须是 777。"""
    repo, _pusher, _uc, svc = guild_stack
    resp = await svc.CreateGuild(guild_pb2.CreateGuildRequest(name="A"), ctx_player(777))
    assert resp.code == errcode_pb2.OK
    assert repo.guilds[resp.guild_id].leader_id == 777


async def test_name_too_long_is_invalid_arg(guild_stack) -> None:
    """长度按 **rune** 算:24 个汉字合法,25 个不合法。按字节算的话 9 个汉字就被拒。"""
    _repo, _pusher, _uc, svc = guild_stack
    ok = await svc.CreateGuild(guild_pb2.CreateGuildRequest(name="公" * 24), ctx_player(1))
    assert ok.code == errcode_pb2.OK
    bad = await svc.CreateGuild(guild_pb2.CreateGuildRequest(name="公" * 25), ctx_player(2))
    assert bad.code == errcode_pb2.ERR_INVALID_ARG


async def test_get_my_guild_returns_ok_with_empty_guild(guild_stack) -> None:
    """★ 不在任何公会是**正常态**:code=OK 且 guild 为空,不是 NOT_FOUND。"""
    _repo, _pusher, _uc, svc = guild_stack
    resp = await svc.GetMyGuild(guild_pb2.GetMyGuildRequest(), ctx_player(999))
    assert resp.code == errcode_pb2.OK
    assert not resp.HasField("guild")


async def test_get_guild_missing_is_not_found(guild_stack) -> None:
    _repo, _pusher, _uc, svc = guild_stack
    resp = await svc.GetGuild(guild_pb2.GetGuildRequest(guild_id=4242), FakeContext())
    assert resp.code == errcode.ErrGuildNotFound


async def test_leader_cannot_leave(guild_stack) -> None:
    _repo, _pusher, _uc, svc = guild_stack
    await svc.CreateGuild(guild_pb2.CreateGuildRequest(name="A"), ctx_player(1))
    resp = await svc.LeaveGuild(guild_pb2.LeaveGuildRequest(), ctx_player(1))
    assert resp.code == errcode.ErrGuildNotLeader


async def test_kick_self_is_invalid_arg(guild_stack) -> None:
    _repo, _pusher, _uc, svc = guild_stack
    await svc.CreateGuild(guild_pb2.CreateGuildRequest(name="A"), ctx_player(1))
    resp = await svc.KickMember(guild_pb2.KickMemberRequest(target_id=1), ctx_player(1))
    assert resp.code == errcode_pb2.ERR_INVALID_ARG


# ── GetPlayerGuild 的两道门 ─────────────────────────────────────────────────


async def test_get_player_guild_rejects_client_jwt(guild_stack) -> None:
    """★ systemOnly:带玩家 JWT 的调用一律拒。

    Envoy 按整前缀路由,"内部方法"在客户端面同样可达 —— 少了这道门,
    它就是「查任意玩家属于哪个公会」的 IDOR 口子。
    """
    _repo, _pusher, _uc, svc = guild_stack
    resp = await svc.GetPlayerGuild(
        guild_pb2.GetPlayerGuildRequest(player_id=5), ctx_player(5)
    )
    assert resp.code == errcode_pb2.ERR_PERMISSION_DENY


async def test_get_player_guild_enforce_requires_ds_token(guild_stack) -> None:
    """★ systemOnly 只证明「不带玩家 JWT」,证明不了「调用方是 DS」。"""
    _repo, _pusher, _uc, svc = guild_stack
    svc.set_ds_callback_guard(make_guard("enforce"))
    resp = await svc.GetPlayerGuild(
        guild_pb2.GetPlayerGuildRequest(player_id=5), FakeContext()
    )
    assert resp.code == errcode_pb2.ERR_UNAUTHORIZED


async def test_get_player_guild_with_ds_token_returns_authoritative_id(guild_stack) -> None:
    repo, _pusher, _uc, svc = guild_stack
    svc.set_ds_callback_guard(make_guard("enforce"))
    await repo.create_guild(11, 500, "A", 100)
    ctx = FakeContext(authorization=f"Bearer {make_ds_token()}")
    resp = await svc.GetPlayerGuild(guild_pb2.GetPlayerGuildRequest(player_id=500), ctx)
    assert resp.code == errcode_pb2.OK and resp.has_guild and resp.guild_id == 11
    # 不在公会是正常态:has_guild=False + OK
    resp2 = await svc.GetPlayerGuild(guild_pb2.GetPlayerGuildRequest(player_id=501), ctx)
    assert resp2.code == errcode_pb2.OK and resp2.has_guild is False


async def test_get_player_guild_bypasses_the_player_facing_cache(guild_stack) -> None:
    """★ DS 反查必须走**权威**读,不吃玩家面板那条 cache-aside 缓存。

    DS 只在进场时查这一次,陈旧值写到实体上会整场不再纠正(会友被显示成路人)。
    这里给一个"缓存说他在 999 号公会"的假缓存,权威说是 11 —— 必须回 11。
    """
    repo = FakeGuildRepo()
    await repo.create_guild(11, 500, "A", 100)

    class LyingCache(ExplodingCache):
        async def get_member_guild_id(self, pid):  # noqa: ANN001
            return 999

        async def get_guild(self, gid):  # noqa: ANN001
            return grows.GuildRow(999, "Stale", 1, 1, 100, 0)

        async def set_guild(self, g, ttl):  # noqa: ANN001
            return None

        async def set_member_guild_id(self, pid, gid, ttl):  # noqa: ANN001
            return None

    uc = gbiz.GuildUsecase(repo, LyingCache(), None, make_cfg())
    svc = gsvc.GuildService(uc, FakeSnowflake(), FakeSnowflake())
    resp = await svc.GetPlayerGuild(
        guild_pb2.GetPlayerGuildRequest(player_id=500), FakeContext()
    )
    assert resp.guild_id == 11, "DS 反查读到了缓存的陈旧值"
    # 对照:玩家面板那条**允许**吃缓存(所以才会读到 999)
    my = await svc.GetMyGuild(guild_pb2.GetMyGuildRequest(), ctx_player(500))
    assert my.guild.guild_id == 999


# ══ ⑤ 弱依赖:缓存 / kafka 坏掉只降级 ═════════════════════════════════════


async def test_cache_failures_degrade_to_mysql() -> None:
    """★ 缓存每个方法都炸,业务仍必须成功(权威读走 MySQL)。"""
    repo = FakeGuildRepo()
    await repo.create_guild(7, 100, "A", 100)
    uc = gbiz.GuildUsecase(repo, ExplodingCache(), None, make_cfg())
    g = await uc.get_guild(7)
    assert g.guild_id == 7
    my = await uc.get_my_guild(100)
    assert my.guild_id == 7
    assert await uc.get_player_guild_id(100) == 7
    # 写路径也不能被删缓存失败带崩
    await uc.create_guild(200, "B", 8)


async def test_push_failure_does_not_fail_the_write() -> None:
    """★ kafka 是弱依赖:推送炸了业务照样成功(客户端拉取兜底)。"""
    repo = FakeGuildRepo()
    uc = gbiz.GuildUsecase(repo, None, ExplodingPusher(), make_cfg())
    await uc.create_guild(100, "A", 7)
    rid, _ = await repo.create_join_request(1, 7, 200, 100)
    await uc.approve_join(100, rid)  # 不抛
    assert 200 in repo.members


# ══ ⑥ 推送原则 ═════════════════════════════════════════════════════════════


async def test_apply_notifies_managers_but_not_the_applicant() -> None:
    """原则 2:申请通知发给会长 / 官员,**不回发申请人本人**。"""
    repo = FakeGuildRepo()
    pusher = RecordingPusher()
    uc = gbiz.GuildUsecase(repo, None, pusher, make_cfg())
    await uc.create_guild(100, "A", 7)
    rid, _ = await repo.create_join_request(1, 7, 200, 100)
    await uc.approve_join(100, rid)
    await repo.set_role(7, 100, 200, grows.GUILD_ROLE_OFFICER)
    pusher.sent.clear()

    await uc.apply_join(300, 7, 999)
    targets = {t for t, _ in pusher.sent}
    assert targets == {100, 200}, "只有会长 + 官员该收到,且不含申请人"


async def test_disband_notifies_everyone_including_the_leader() -> None:
    """★ 解散是全员事件,**例外于原则 2** —— 会长自己也要收到。"""
    repo = FakeGuildRepo()
    pusher = RecordingPusher()
    uc = gbiz.GuildUsecase(repo, None, pusher, make_cfg())
    await uc.create_guild(100, "A", 7)
    for pid in (200, 201):
        rid, _ = await repo.create_join_request(pid, 7, pid, 100)
        await uc.approve_join(100, rid)
    pusher.sent.clear()

    await uc.disband_guild(100)
    assert {t for t, _ in pusher.sent} == {100, 200, 201}
    assert {e for _, e in pusher.sent} == {guild_pb2.GUILD_EVENT_TYPE_DISBANDED}


async def test_rate_quota_rejects_before_any_side_effect() -> None:
    """★ 频率配额必须**先于一切读写**:被拒时不能留下任何申请行。"""

    class DenyingQuota:
        async def allow(self, action, subject):  # noqa: ANN001
            return False, None          # 契约是 (ok, exc)

    repo = FakeGuildRepo()
    uc = gbiz.GuildUsecase(repo, None, None, make_cfg())
    uc.set_rate_quota(DenyingQuota())
    await repo.create_guild(7, 100, "A", 100)
    with pytest.raises(errcode.PandoraError) as exc:
        await uc.apply_join(300, 7, 999)
    assert exc.value.code == errcode.ErrRateLimited
    assert repo.requests == {}, "被限流却留下了申请行"


async def test_rate_quota_failure_is_fail_open() -> None:
    """★ 配额判定本身出错时 fail-open:这是背压门不是权威门,总量闸仍在事务里守着。"""

    class BrokenQuota:
        async def allow(self, action, subject):  # noqa: ANN001
            # 真实的 ActionQuota 从不抛:故障走返回值,fail-open 放行。
            return True, RuntimeError("redis down")

    repo = FakeGuildRepo()
    uc = gbiz.GuildUsecase(repo, None, None, make_cfg())
    uc.set_rate_quota(BrokenQuota())
    await repo.create_guild(7, 100, "A", 100)
    rid = await uc.apply_join(300, 7, 999)
    assert rid == 999


# ══ ⑦ 群 service ═══════════════════════════════════════════════════════════


class FakeGroupRepo:
    def __init__(self) -> None:
        self.groups: dict[int, grows.GroupRow] = {}
        self.members: dict[tuple[int, int], grows.GroupMemberRow] = {}

    async def create_group(self, gid, owner_id, member_ids, *, name, max_members, max_groups_per_player):  # noqa: ANN001
        self.groups[gid] = grows.GroupRow(gid, name, owner_id, 1 + len(member_ids), max_members, 0)
        self.members[(gid, owner_id)] = grows.GroupMemberRow(
            gid, owner_id, group_pb2.GROUP_ROLE_OWNER, 0
        )
        for pid in member_ids:
            self.members[(gid, pid)] = grows.GroupMemberRow(
                gid, pid, group_pb2.GROUP_ROLE_MEMBER, 0
            )

    async def get_group(self, gid):  # noqa: ANN001
        return self.groups.get(gid)

    async def get_group_member(self, gid, pid):  # noqa: ANN001
        return self.members.get((gid, pid))

    async def add_member(self, gid, pid, *, operator_id, max_members, max_groups_per_player):  # noqa: ANN001
        if (gid, pid) in self.members:
            return True
        self.members[(gid, pid)] = grows.GroupMemberRow(
            gid, pid, group_pb2.GROUP_ROLE_MEMBER, 0
        )
        return False

    async def remove_member(self, gid, pid):  # noqa: ANN001
        self.members.pop((gid, pid), None)

    async def kick_member(self, gid, op, target):  # noqa: ANN001
        self.members.pop((gid, target), None)

    async def disband_group(self, gid, op):  # noqa: ANN001
        for k in [k for k in self.members if k[0] == gid]:
            self.members.pop(k)
        self.groups.pop(gid, None)

    async def transfer_owner(self, gid, old, new):  # noqa: ANN001
        self.members[(gid, old)].role = group_pb2.GROUP_ROLE_MEMBER
        self.members[(gid, new)].role = group_pb2.GROUP_ROLE_OWNER
        self.groups[gid].owner_id = new

    async def list_group_members(self, gid):  # noqa: ANN001
        return sorted(
            (m for k, m in self.members.items() if k[0] == gid), key=lambda m: m.role
        )

    async def list_my_group_rows(self, pid):  # noqa: ANN001
        return [self.groups[k[0]] for k in self.members if k[1] == pid]


@pytest.fixture
def group_stack():
    repo = FakeGroupRepo()
    uc = gbiz.GroupUsecase(repo, make_cfg())
    return repo, gsvc.GroupService(uc, FakeSnowflake(300))


async def test_create_group_dedups_and_excludes_owner(group_stack) -> None:
    repo, svc = group_stack
    resp = await svc.CreateGroup(
        group_pb2.CreateGroupRequest(name="G", member_ids=[1, 2, 2, 0, 1]), ctx_player(1)
    )
    assert resp.code == errcode_pb2.OK
    gid = resp.group_id
    assert sorted(k[1] for k in repo.members if k[0] == gid) == [1, 2]


async def test_group_owner_cannot_leave(group_stack) -> None:
    repo, svc = group_stack
    resp = await svc.CreateGroup(group_pb2.CreateGroupRequest(name="G"), ctx_player(1))
    gid = resp.group_id
    out = await svc.LeaveGroup(group_pb2.LeaveGroupRequest(group_id=gid), ctx_player(1))
    assert out.code == errcode.ErrGroupNotOwner


async def test_non_owner_cannot_disband(group_stack) -> None:
    repo, svc = group_stack
    gid = (await svc.CreateGroup(
        group_pb2.CreateGroupRequest(name="G", member_ids=[2]), ctx_player(1)
    )).group_id
    out = await svc.DisbandGroup(group_pb2.DisbandGroupRequest(group_id=gid), ctx_player(2))
    assert out.code == errcode.ErrGroupNotOwner


async def test_member_can_invite(group_stack) -> None:
    """★ 与公会不同:临时群**允许成员拉人**(只有 owner/member 两级)。"""
    repo, svc = group_stack
    gid = (await svc.CreateGroup(
        group_pb2.CreateGroupRequest(name="G", member_ids=[2]), ctx_player(1)
    )).group_id
    out = await svc.InviteToGroup(
        group_pb2.InviteToGroupRequest(group_id=gid, target_id=3), ctx_player(2)
    )
    assert out.code == errcode_pb2.OK
    assert (gid, 3) in repo.members


async def test_outsider_cannot_invite(group_stack) -> None:
    repo, svc = group_stack
    gid = (await svc.CreateGroup(group_pb2.CreateGroupRequest(name="G"), ctx_player(1))).group_id
    out = await svc.InviteToGroup(
        group_pb2.InviteToGroupRequest(group_id=gid, target_id=3), ctx_player(9)
    )
    assert out.code == errcode.ErrGroupNotMember


async def test_group_rpcs_require_identity(group_stack) -> None:
    _repo, svc = group_stack
    assert (
        await svc.ListMyGroups(group_pb2.ListMyGroupsRequest(), FakeContext())
    ).code == errcode_pb2.ERR_UNAUTHORIZED
    assert (
        await svc.CreateGroup(group_pb2.CreateGroupRequest(name="G"), FakeContext())
    ).code == errcode_pb2.ERR_UNAUTHORIZED


# ══ ⑧ 取消必须穿透(优雅停机)═══════════════════════════════════════════════


async def test_cancellation_propagates_instead_of_becoming_a_business_code() -> None:
    """★ CancelledError 是 BaseException,被宽 except 吞掉的话:
    优雅停机会把「取消」映射成 in-band 业务码并返回一个**正常响应**,
    客户端每次滚动更新都收到一批假失败,而排空在途并没有真的发生。
    """

    class CancellingRepo(FakeGuildRepo):
        async def get_guild(self, gid):  # noqa: ANN001
            raise asyncio.CancelledError

    uc = gbiz.GuildUsecase(CancellingRepo(), None, None, make_cfg())
    svc = gsvc.GuildService(uc, FakeSnowflake(), FakeSnowflake())
    with pytest.raises(asyncio.CancelledError):
        await svc.GetGuild(guild_pb2.GetGuildRequest(guild_id=1), FakeContext())


# ══ ⑨ main 可导入 / 命令行形状 ═════════════════════════════════════════════


def test_main_module_imports_and_parses_go_style_flag() -> None:
    """★ 必须能被 `-conf`(**单**横线)拉起:run_services.ps1 / K8s manifest
    里全是这个形状,改成 `--conf` 等于所有部署脚本都要改。
    """
    from pandorapy.services.guild import main as gmain

    assert gmain._parse_args(["-conf", "x.yaml"]).conf == "x.yaml"
    assert gmain._parse_args([]).conf == "etc/guild-dev.yaml"
    assert gmain.HTTP_DEFAULT_PORT == 21008
    assert gmain.GRPC_SERVICE_FULL_NAMES == (
        "pandora.guild.v1.GuildService",
        "pandora.group.v1.GroupService",
    )


def test_all_23_rpcs_are_implemented() -> None:
    """★ 两个 servicer 一共 23 个 RPC,少一个就是 UNIMPLEMENTED 而不是报错。"""
    from pandora.group.v1 import group_pb2_grpc
    from pandora.guild.v1 import guild_pb2_grpc

    def declared(servicer_cls) -> set[str]:  # noqa: ANN001
        return {n for n in vars(servicer_cls) if n[:1].isupper()}

    guild_rpcs = declared(guild_pb2_grpc.GuildServiceServicer)
    group_rpcs = declared(group_pb2_grpc.GroupServiceServicer)
    assert len(guild_rpcs) == 14 and len(group_rpcs) == 9
    for name in guild_rpcs:
        assert name in vars(gsvc.GuildService), f"GuildService 缺 {name}"
    for name in group_rpcs:
        assert name in vars(gsvc.GroupService), f"GroupService 缺 {name}"


def test_service_module_has_no_grpc_abort() -> None:
    """★ 业务失败必须走 in-band code。`abort` 会让调用方走到完全不同的错误分支。

    走 AST 而不是字符串匹配:注释和 docstring 里就写着"不要用 context.abort()",
    按字符串扫会被自己的说明文字绊倒(第一版正是这样红的)。
    """
    import ast

    tree = ast.parse(pathlib.Path(gsvc.__file__).read_text(encoding="utf-8"))
    banned = {"abort", "abort_with_status", "set_code", "set_details"}
    hits = [
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in banned
    ]
    assert not hits, f"service 层调用了 {hits} —— 业务失败必须回 in-band code"
