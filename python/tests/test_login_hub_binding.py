"""Hub DSTicket 归属绑定门的回归测试。

★ 这些用例锁的是一条**曾经被整段删掉**的校验(见 `hubbinding.py` 顶部)。
移植初版只留了"验签 + 会话 + jti",于是:

  - A 玩家的 Hub 票能在 B 台 DS 上兑换;
  - Transfer / Release / 同名 Pod 重建后的旧票永久有效;
  - 半绑定票(签发面半截升级)被当合法票放行。

所以每条用例都写成"**删掉对应代码就会红**"的形状,而不是只验一遍正路。
"""

from __future__ import annotations

import base64
import json

import pytest

from pandora.hub.v1 import allocator_pb2 as hubpb

from pandorapy import errcode
from pandorapy.auth import DS_AUTH_WRITER_EPOCH_V2
from pandorapy.services.login import biz as lbiz
from pandorapy.services.login import dsticket as ldsticket
from pandorapy.services.login import hubbinding as hb

POD = "hub-0"
UID = "uid-abc"
EPOCH = 7
GEN = 3
CJTI = "cred-jti"
AID = "assign-1"
TRACK = "stable"
NOW_MS = 1_700_000_000_000


def _binding(**over) -> hb.HubAssignmentBinding:
    base = dict(
        pod_name=POD, instance_uid=UID, protocol_epoch=EPOCH, credential_gen=GEN,
        credential_jti=CJTI, assignment_id=AID,
        writer_epoch=DS_AUTH_WRITER_EPOCH_V2, release_track=TRACK,
    )
    base.update(over)
    return hb.HubAssignmentBinding(**base)


def _claims(**over) -> ldsticket.DSTicketClaims:
    base = dict(
        player_id=42, ds_type=ldsticket.DS_TYPE_HUB, jti="tkt-jti",
        ds_pod_name=POD, ds_instance_uid=UID, ds_protocol_epoch=EPOCH,
        ds_credential_gen=GEN, ds_credential_jti=CJTI, hub_assignment_id=AID,
        ds_writer_epoch=DS_AUTH_WRITER_EPOCH_V2, release_track=TRACK,
    )
    base.update(over)
    return ldsticket.DSTicketClaims(**base)


# ── binding 形态判别 ────────────────────────────────────────────────

def test_half_binding_is_neither_complete_nor_empty() -> None:
    """★ 半绑定必须两边都不沾 —— 这是"半截升级比无绑定更危险"的机械表达。

    如果 `empty()` 写成 `not complete()`,半绑定票会掉进"兼容旧票"分支被放行。
    """
    half = hb.HubAssignmentBinding(pod_name=POD, instance_uid=UID)
    assert not half.complete()
    assert not half.empty()


def test_release_track_participates_in_empty() -> None:
    """只有 release_track 的票也算半绑定,不是空票。"""
    assert not hb.HubAssignmentBinding(release_track="canary").empty()


def test_binding_from_claims_carries_release_track() -> None:
    """★ claims 必须解出 release_track。

    初版 `dsticket.py` 漏了这一列,于是归属校验里所有 release_track 判据都因为
    恒为空串被跳过 —— stable 票能在 canary Pod 上兑换。
    """
    b = hb.binding_from_claims(_claims())
    assert b.release_track == TRACK
    assert b.complete()


# ── Redis 权威校验 ──────────────────────────────────────────────────

class _FakeRedis:
    """按 key 返回预置字节;可注入异常与"第二次读变了"。"""

    def __init__(self, values: dict[str, bytes], *, get_exc=None, mget_exc=None):
        self.values = values
        self.get_exc = get_exc
        self.mget_exc = mget_exc
        self.get_calls = 0
        self.second_assignment: bytes | None = None

    async def get(self, key: str):
        self.get_calls += 1
        if self.get_exc is not None:
            raise self.get_exc
        if self.get_calls == 2 and self.second_assignment is not None:
            return self.second_assignment
        return self.values.get(key)

    async def mget(self, *keys):
        if self.mget_exc is not None:
            raise self.mget_exc
        return [self.values.get(k) for k in keys]


def _authority_bytes(*, heartbeat_ms=NOW_MS - 1000, state="ready", track=TRACK):
    assignment = hubpb.HubAssignmentStorageRecord(
        player_id=42, assignment_id=AID, hub_pod_name=POD, hub_instance_uid=UID,
        auth_epoch=EPOCH, auth_gen=GEN, auth_jti=CJTI,
        auth_writer_epoch=DS_AUTH_WRITER_EPOCH_V2, release_track=track,
    )
    auth = hubpb.HubShardAuthStorageRecord(
        pod_name=POD, instance_uid=UID, protocol_epoch=EPOCH,
        phase=hubpb.HubAuthPhase.HUB_AUTH_PHASE_ACTIVE,
        required_writer_epoch=DS_AUTH_WRITER_EPOCH_V2, high_water_gen=GEN,
        last_active_heartbeat_ms=heartbeat_ms,
    )
    auth.active.instance_uid = UID
    auth.active.protocol_epoch = EPOCH
    auth.active.gen = GEN
    auth.active.jti = CJTI
    auth.active.writer_epoch = DS_AUTH_WRITER_EPOCH_V2
    auth.active.exp_ms = NOW_MS + 60_000
    auth.active.kid = "kid-1"
    auth.active.token_sha256 = "sha-1"
    shard = hubpb.HubShardStorageRecord(
        hub_pod_name=POD, state=state, release_track=track, gameserver_uid=UID,
        auth_epoch=EPOCH, last_verified_gen=GEN, last_verified_jti=CJTI,
        last_verified_writer_epoch=DS_AUTH_WRITER_EPOCH_V2,
        last_heartbeat_ms=heartbeat_ms,
    )
    return {
        hb.hub_player_assignment_key(42): assignment.SerializeToString(),
        hb.hub_auth_authority_key(POD): auth.SerializeToString(),
        hb.hub_shard_projection_key(POD): shard.SerializeToString(),
    }


def _checker(values, **kw) -> hb.RedisHubAssignmentChecker:
    return hb.RedisHubAssignmentChecker(
        _FakeRedis(values, **kw) if not isinstance(values, _FakeRedis) else values,
        now=lambda: NOW_MS / 1000.0,
    )


@pytest.mark.asyncio
async def test_matching_authority_passes() -> None:
    await _checker(_authority_bytes()).check_current(42, _binding())


@pytest.mark.asyncio
async def test_assignment_missing_is_ticket_invalid_not_unavailable() -> None:
    """★ 方向判据:票据无效 vs 权威不可判定,不能互相冒充。"""
    with pytest.raises(errcode.PandoraError) as ei:
        await _checker({}).check_current(42, _binding())
    assert ei.value.code == errcode.ErrLoginTicketInvalid


@pytest.mark.asyncio
async def test_redis_failure_is_unavailable_not_pass_and_not_invalid() -> None:
    """★ 这条是整个文件里最要紧的一条。

    Redis 故障压成"通过" → 一次抖动窗口内**全部旧票集体可用**;
    压成"票据无效" → 一次抖动把大批在线玩家踢成需要重登。
    两个方向都错,必须是 UNAVAILABLE(§9.22:UNKNOWN 不得冒充任何确定态)。
    """
    with pytest.raises(errcode.PandoraError) as ei:
        await _checker({}, get_exc=RuntimeError("connection reset")).check_current(42, _binding())
    assert ei.value.code == errcode.ErrUnavailable


@pytest.mark.asyncio
async def test_corrupt_protobuf_is_unavailable() -> None:
    """坏 protobuf = 权威不可判定,同上。"""
    vals = _authority_bytes()
    vals[hb.hub_player_assignment_key(42)] = b"\xff\xff\xff\xff not-a-proto"
    with pytest.raises(errcode.PandoraError) as ei:
        await _checker(vals).check_current(42, _binding())
    assert ei.value.code == errcode.ErrUnavailable


@pytest.mark.asyncio
async def test_assignment_changed_mid_validation_is_rejected() -> None:
    """★ A1 → MGET → A2 双采集的线性化证明。

    assignment 与 {pod} 槽不同 slot,不能 MULTI。中途被 Transfer 改过 → 拒。
    删掉第二次读(A2)这条就会绿,所以它锁的正是那段代码。
    """
    fake = _FakeRedis(_authority_bytes())
    fake.second_assignment = b"different-payload"
    with pytest.raises(errcode.PandoraError) as ei:
        await _checker(fake).check_current(42, _binding())
    assert ei.value.code == errcode.ErrLoginTicketInvalid


@pytest.mark.asyncio
async def test_stale_heartbeat_is_rejected() -> None:
    """心跳超龄 = 那台 DS 已经不算活着,票不能兑。"""
    with pytest.raises(errcode.PandoraError):
        await _checker(_authority_bytes(heartbeat_ms=NOW_MS - 60_000)).check_current(42, _binding())


@pytest.mark.asyncio
async def test_draining_shard_is_rejected_even_if_assignment_matches() -> None:
    """★ 入场不能只看 auth:shard 已 draining 时即使 assignment 还没迁走也必须拒。"""
    with pytest.raises(errcode.PandoraError):
        await _checker(_authority_bytes(state="draining")).check_current(42, _binding())


@pytest.mark.asyncio
async def test_release_track_mismatch_is_rejected() -> None:
    """§9.21 灰度轨道粘滞:stable 票不能在 canary Pod 上兑换。"""
    with pytest.raises(errcode.PandoraError):
        await _checker(_authority_bytes(track="canary")).check_current(42, _binding())


# ── TicketUsecase 门本身 ────────────────────────────────────────────

def _hs256_token(sub: str = "42") -> str:
    """构一张 **JOSE header 可解析的 HS256 票**,签名是垃圾。

    ★ 不能再像初版那样传字面串 `"tkt"`。`_verify_ds_ticket_signature` 的第一步是
      `ds_ticket_algorithm(ticket)` —— 先解 JOSE header 才知道该选 HS256 还是
      RS256 verifier(算法混淆的第一道闸)。`"tkt"` 没有 header,会在**进入本文件
      要测的归属绑定门之前**就以 `ds ticket header invalid` 拒掉,于是每条用例都在
      验一个它没打算验的东西 —— 假件根本没把代码送进被测状态。

      签名是垃圾没关系:`_Signer.verify` 忽略入参直接返回预置 claims,本文件锁的是
      **验签之后**的归属绑定 / 会话 / jti 三段,不是验签本身。

    `sub` 只用来让"两张不同的票"在字节上真的不同(防重放用例要区分票体)。
    """
    def seg(obj: dict) -> str:
        # JWT 用**无 padding** base64url。带 `=` 会让 PyJWT 解不出 header。
        raw = json.dumps(obj, separators=(",", ":")).encode()
        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()

    return seg({"alg": "HS256", "typ": "JWT"}) + "." + seg({"sub": sub}) + ".c2ln"


TKT = _hs256_token()
TKT_A = _hs256_token("a")
TKT_B = _hs256_token("b")


class _Signer:
    ttl = 300

    def __init__(self, claims):
        self._claims = claims

    def verify(self, token):  # noqa: ANN001
        del token
        return self._claims


class _OkChecker:
    def __init__(self):
        self.calls = 0

    async def check_current(self, player_id, expected):  # noqa: ANN001
        del player_id, expected
        self.calls += 1


@pytest.mark.asyncio
async def test_pod_mismatch_is_rejected_before_authority_lookup() -> None:
    """★ 票内 ds_pod ≠ 调用方 pod → 拒,且**不查权威**(不消耗一次性资源)。

    这一条没有的话,A 玩家的 Hub 票能在 B 台 DS 上兑换。
    """
    checker = _OkChecker()
    uc = lbiz.TicketUsecase(_Signer(_claims()), None)
    uc.set_hub_assignment_checker(checker)
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.verify_ds_ticket(TKT, "hub-OTHER")
    assert ei.value.code == errcode.ErrUnauthorized
    assert checker.calls == 0, "pod 不匹配就不该再去查权威"


@pytest.mark.asyncio
async def test_empty_caller_pod_is_rejected() -> None:
    with pytest.raises(errcode.PandoraError) as ei:
        uc = lbiz.TicketUsecase(_Signer(_claims()), None)
        uc.set_hub_assignment_checker(_OkChecker())
        await uc.verify_ds_ticket(TKT, "")
    assert ei.value.code == errcode.ErrUnauthorized


@pytest.mark.asyncio
async def test_half_binding_ticket_is_rejected() -> None:
    """半绑定 = 签发面半截升级,比无绑定更危险,不能当兼容旧票放过。"""
    uc = lbiz.TicketUsecase(_Signer(_claims(ds_credential_jti="", hub_assignment_id="")), None)
    uc.set_hub_assignment_checker(_OkChecker())
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.verify_ds_ticket(TKT, POD)
    assert ei.value.code == errcode.ErrLoginTicketInvalid


@pytest.mark.asyncio
async def test_missing_checker_with_complete_binding_fails_closed() -> None:
    """★ 校验器没装 = 权威不可判定 → UNAVAILABLE,**不是放行**。"""
    uc = lbiz.TicketUsecase(_Signer(_claims()), None)
    uc.set_hub_assignment_checker(None)
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.verify_ds_ticket(TKT, POD)
    assert ei.value.code == errcode.ErrUnavailable


@pytest.mark.asyncio
async def test_empty_binding_passes_when_fence_off_and_rejected_when_on() -> None:
    """空绑定旧票的两个方向,由 require_hub_assignment_binding 栅栏决定。"""
    empty = _claims(
        ds_pod_name="", ds_instance_uid="", ds_protocol_epoch=0, ds_credential_gen=0,
        ds_credential_jti="", hub_assignment_id="", ds_writer_epoch=0, release_track="",
    )
    uc = lbiz.TicketUsecase(_Signer(empty), None)
    uc.set_hub_assignment_checker(None, require_binding=False)
    assert (await uc.verify_ds_ticket(TKT, POD)).player_id == 42

    uc.set_hub_assignment_checker(None, require_binding=True)
    with pytest.raises(errcode.PandoraError) as ei:
        await uc.verify_ds_ticket(TKT, POD)
    assert ei.value.code == errcode.ErrLoginTicketInvalid


@pytest.mark.asyncio
async def test_battle_ticket_skips_hub_binding() -> None:
    """battle 票不走 hub 归属门(它有自己的 roster 权威链)。"""
    uc = lbiz.TicketUsecase(
        _Signer(_claims(ds_type=ldsticket.DS_TYPE_BATTLE, match_id=9)), None
    )
    uc.set_hub_assignment_checker(None)
    assert (await uc.verify_ds_ticket(TKT, "battle-0")).match_id == 9


# ── jti 空串防重放 ──────────────────────────────────────────────────

class _JTIRepo:
    def __init__(self):
        self.used: list[str] = []

    async def mark_used(self, jti, ttl_sec):  # noqa: ANN001
        del ttl_sec
        if jti in self.used:
            raise errcode.PandoraError(errcode.ErrLoginTicketInvalid, "replayed")
        self.used.append(jti)


@pytest.mark.asyncio
async def test_empty_jti_does_not_mint_a_shared_replay_key() -> None:
    """★ pyjwt 的 `require: [jti]` 只保证键**存在**,不保证非空。

    没有 `claims.jti != ""` 前置判据的话,空串会在 Redis 上铸一个**全局共享**的
    防重放键:第一张空 jti 票畅通,第二张起全被判重放 —— 而且两张票毫无关系。
    """
    repo = _JTIRepo()
    uc = lbiz.TicketUsecase(_Signer(_claims(jti="", ds_type=ldsticket.DS_TYPE_BATTLE)), repo)
    uc.set_hub_assignment_checker(None)
    await uc.verify_ds_ticket(TKT_A, "battle-0")
    await uc.verify_ds_ticket(TKT_B, "battle-0")  # 第二张不该被判重放
    assert repo.used == [], "空 jti 不该写任何防重放键"
