"""hub_allocator 容量账本存取层回归测试 —— 覆盖 `services/hub_allocator/ledger.py`。

对应 Go 侧 `internal/data/hub_capacity_ledger.go` 及其 `hub_capacity_ledger_test.go`。

★ **优先用真 Redis**(默认 `127.0.0.1:16379`,docker 容器 `pandora-redis`),
连不上才回落 `fakeredis`。与 `test_hub_allocator_repo.py` 的"连不上就 skip"不同 ——
那份测的是 `SET NX` / `PTTL` 三态这类 fake 最容易"差不多对"的语义,skip 才诚实;
本文件测的是**账本记账规则**(谁计容、谁不计容、谁永不过期),WATCH/MULTI/EXEC
只是承载它的事务壳。为这些规则留一条"环境不好就整体不跑"的后门,等于把
§9.22(唯一权威 / 不重复影子状态)最核心的几条断言变成可选项:

    docker run -d --name pandora-redis -p 16379:6379 redis:8-alpine

★ 每个用例用**独立的 pod 名 / player_id 段**,避免跨用例污染。
★ 每条用例的 docstring 里 `★ 变异:` 一行记录"改坏哪一行会让本条变红" ——
  都是真跑过的(改坏 → 红 → 改回 → 绿),不是照着代码猜的。
"""

from __future__ import annotations

import asyncio
import base64
import os
import uuid

import pytest
from pandora.hub.v1 import allocator_pb2 as hubpb

from pandorapy import errcode
from pandorapy.auth import DS_AUTH_WRITER_EPOCH_V2 as WRITER
from pandorapy.protoenum import enum_name
from pandorapy.services.hub_allocator import auth_repo as hauth
from pandorapy.services.hub_allocator import ledger as L

# ── fixture ─────────────────────────────────────────────────────────────────

TTL = 600.0

# base64 RawURL 字母表(与 `successor_capability` 的编码一致)。
_B64URL = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"


def _non_canonical_variants(cap: str) -> list[str]:
    """造出「解码结果相同、字节串不同」的 base64 变体。

    这正是回环校验要挡的东西:不做重编码比对,同一个座位就能有两条都解得开的
    capability,于是重复计容,而且其中一条永远匹配不上 canonical field,清不掉。

    两类变体(按 canonical 串长度 mod 4,可能只存在其中一类,也可能都不存在):
      ① **填充变体** —— 编码器用 RawURL(无 `=`),补回 `=` 后仍能解码。
      ② **未使用 bit 变体** —— 末字符只有一部分 bit 承载数据,翻转末位 bit
         得到另一个字符,解码丢弃这些 bit,结果字节完全相同。
    """
    out: list[str] = []
    pad = "=" * (-len(cap) % 4)
    if pad:
        out.append(cap + pad)
    raw = base64.urlsafe_b64decode(cap + pad)
    if len(raw) % 3:  # 末字符存在未使用低位时才有 ② 类变体
        flipped = _B64URL[_B64URL.index(cap[-1]) ^ 1]
        out.append(cap[:-1] + flipped)
    return out


@pytest.fixture
async def rdb():
    """独占一个空的 Redis 逻辑库;真 Redis 不可用时回落 fakeredis(不 skip)。"""
    import redis.asyncio as aioredis

    addr = os.getenv("PANDORA_TEST_REDIS_ADDR", "127.0.0.1:16379")
    host, _, port = addr.rpartition(":")

    client = None
    for i in range(16):
        db = (os.getpid() + i) % 16
        candidate = aioredis.Redis(
            host=host or "127.0.0.1",
            port=int(port or 16379),
            db=db,
            decode_responses=False,
            socket_connect_timeout=3,
            socket_timeout=3,
        )
        try:
            await asyncio.wait_for(candidate.ping(), timeout=4)
        except Exception:  # noqa: BLE001 —— 连不上/超时都回落,原因不影响决策
            await candidate.aclose()
            client = None
            break
        if await candidate.dbsize() == 0:
            client = candidate
            break
        await candidate.aclose()
    if client is None:
        from fakeredis import aioredis as fake

        client = fake.FakeRedis(decode_responses=False)
    try:
        yield client
    finally:
        await client.flushdb()
        await client.aclose()


class FakeLease:
    """写者租约替身。

    ★ 返回顺序**照 `pandorapy.writerlease.Lease`**:`(held, token)`。
      Go 的 `WriterFence.Current()` 是反过来的 `(token, held)` —— 这份替身刻意
      不迁就 Go,它就是用来钉死"生产代码必须按 Python 侧顺序解包"的。
    """

    def __init__(self, token: int, held: bool = True) -> None:
        self.token = token
        self.held = held

    def current(self) -> tuple[bool, int]:
        return self.held, self.token


# ── 构造辅助 ────────────────────────────────────────────────────────────────


def _cred(*, uid: str, epoch: int, gen: int, jti: str, at_ms: int, kid: str = "kid-1"):
    return hubpb.HubDSCredential(
        gen=gen,
        jti=jti,
        exp_ms=at_ms + 3_600_000,
        kid=kid,
        instance_uid=uid,
        protocol_epoch=epoch,
        token_sha256="sha-" + jti,
        writer_epoch=WRITER,
    )


def _ident(cred) -> L.CredentialIdentity:
    return L.CredentialIdentity(
        gen=cred.gen,
        jti=cred.jti,
        instance_uid=cred.instance_uid,
        protocol_epoch=cred.protocol_epoch,
        token_sha256=cred.token_sha256,
        kid=cred.kid,
        writer_epoch=cred.writer_epoch,
    )


def _reservation(
    player_id: int,
    assignment_id: str,
    *,
    uid: str,
    at_ms: int,
    epoch: int = 1,
    version: int = 0,
    operation_id: str = "",
    source_match_id: int = 0,
) -> L.ReservationIdentity:
    return L.ReservationIdentity(
        player_id=player_id,
        assignment_id=assignment_id,
        instance_uid=uid,
        protocol_epoch=epoch,
        writer_epoch=WRITER,
        placement_version=version,
        placement_operation_id=operation_id,
        source_match_id=source_match_id,
        expires_at_ms=at_ms + 60_000,
        assignment_expires_at_ms=at_ms + 120_000,
    )


def _res_rec(player_id: int, assignment_id: str, *, pod: str, uid: str, expires_at_ms: int):
    return hubpb.HubReservationStorageRecord(
        player_id=player_id,
        assignment_id=assignment_id,
        hub_pod_name=pod,
        hub_instance_uid=uid,
        auth_epoch=1,
        auth_writer_epoch=WRITER,
        expires_at_ms=expires_at_ms,
    )


def _session_rec(player_id: int, assignment_id: str, *, pod: str, uid: str, expires_at_ms: int = 0):
    return hubpb.HubConnectedOwnershipStorageRecord(
        player_id=player_id,
        assignment_id=assignment_id,
        admission_id=str(uuid.uuid4()),
        hub_pod_name=pod,
        hub_instance_uid=uid,
        auth_epoch=1,
        auth_writer_epoch=WRITER,
        admission_seq=1,
        expires_at_ms=expires_at_ms,
    )


async def _bootstrap(rdb, pod: str, *, capacity: int = 500, uid: str = "uid-a"):
    """把一台 Hub 引导到「授权记录 ACTIVE + 分片 ready」的可路由状态。

    刻意走真实链路(init_auth → stage_pending → activate_heartbeat)而不是手搓
    Redis 字节:手搓出来的记录只要漏填一个投影字段,后面的用例就会因为
    `modelb_routable_reason` 返回一个与被测点无关的 reason 而"绿得莫名其妙"。
    """
    repo = hauth.RedisHubAuthRepo(rdb)
    at = L.now_ms()
    await repo.init_auth(pod, uid, TTL)
    cred = _cred(uid=uid, epoch=1, gen=1, jti="j1", at_ms=at)
    await repo.stage_pending(pod, cred, TTL)
    shard = hubpb.HubShardStorageRecord(
        hub_pod_name=pod,
        hub_addr="10.0.0.1:7777",
        region="cn",
        shard_id=1,
        capacity=capacity,
        state="warming",
    )
    await rdb.set(L.shard_key(pod), shard.SerializeToString())
    out = await repo.activate_heartbeat(
        pod,
        _ident(cred),
        hauth.ActivateHeartbeatInput(
            max_players=capacity, state="ready", auth_ttl_sec=TTL, shard_ttl_sec=TTL
        ),
    )
    assert out.accepted, "引导失败:后续用例的前提不成立"
    # ★ 取激活**之后**的时刻:`activate_heartbeat` 用服务端 now_ms() 落心跳时刻,
    #   拿激活之前的 at 去问路由会得到 `heartbeat-invalid`(心跳在未来),
    #   于是每条用例都会因为一个与被测点无关的 reason 而红。
    return repo, cred, L.now_ms()


async def _shard_of(rdb, pod: str) -> hubpb.HubShardStorageRecord:
    return L.unmarshal_shard(pod, await rdb.get(L.shard_key(pod)))


# ── ① key 模板与 hashtag(两栈并存 + 单 slot 事务的硬契约)────────────────────


def test_capacity_ledger_keys_share_one_pod_hashtag() -> None:
    """★ 六把 ledger key 必须与 auth / shard / wfence 落**同一个 hashtag**。

    这不是洁癖:`reserve_assignment` 要在一次 WATCH/MULTI/EXEC 里同时读授权记录、
    分片镜像和六把账本键。Redis Cluster 只按 `{...}` 里的内容分 slot,任何一把键
    的 hashtag 写歪,整条事务在集群上直接 CROSSSLOT 报错 —— 而单机 Redis 上
    **完全测不出来**,会一路绿到上生产那天。

    ★ 变异:把 `reservations_key` 的 `{{{pod}}}` 改成 `{{hub}}:{pod}`(或任何让
      hashtag 不再只包 pod 的写法)→ 本条红。
    """
    pod = "t01-a"
    keys = L.capacity_ledger_keys(pod)
    assert keys == [
        "pandora:hub:reservations:{t01-a}",
        "pandora:hub:reservation-expiry:{t01-a}",
        "pandora:hub:sessions:{t01-a}",
        "pandora:hub:session-expiry:{t01-a}",
        "pandora:hub:successors:{t01-a}",
        "pandora:hub:successor-expiry:{t01-a}",
    ]

    def hashtag(key: str) -> str:
        return key[key.index("{") + 1 : key.index("}")]

    same_slot = [
        *keys,
        L.auth_key(pod),
        L.shard_key(pod),
        L.wfence_key(pod),
        L.instance_teardown_proof_key(pod),
    ]
    assert {hashtag(k) for k in same_slot} == {pod}


def test_ledger_key_order_matches_go_watch_set() -> None:
    """★ 六把键的**顺序**也照抄 Go 的 `capacityLedgerKeys`。

    顺序本身不影响 WATCH 语义,但它决定 `write_hub_capacity_ledger` 里那条
    `DEL r, rx, s, sx, x, xx` 的参数排布。两栈对不上时,任何一次跨栈的
    "按位置读第 N 把键"的运维脚本 / 排障 SQL 都会指错表。

    ★ 变异:把 `capacity_ledger_keys` 里 sessions 与 successors 两行对调 → 本条红。
    """
    pod = "t02-a"
    assert L.capacity_ledger_keys(pod) == [
        L.reservations_key(pod),
        L.reservation_expiry_key(pod),
        L.sessions_key(pod),
        L.session_expiry_key(pod),
        L.successors_key(pod),
        L.successor_expiry_key(pod),
    ]
    assert L.instance_teardown_proof_key(pod) == "pandora:hub:instance-teardown:{t02-a}"


# ── ② 凭据四元组:四个字段各写一条,不合并成"全错一起测"────────────────────
#
# 合并成一条的话,只要**任意一个**字段的比较还在,用例就照样绿 —— 而被删掉的
# 那个比较正是被打穿的那道门。所以 gen / jti / instance_uid / protocol_epoch
# 逐个单独立案,每条只动一个字段。


def _matching_pair(at_ms: int):
    cred = _cred(uid="uid-a", epoch=3, gen=7, jti="j7", at_ms=at_ms)
    return cred, _ident(cred)


def test_cred_matches_accepts_the_exact_full_tuple() -> None:
    """★ 基线:完整且逐字段相等的凭据必须通过。

    没有这条基线,下面四条"不匹配就拒"全都可能是假绿(比如 `cred_matches` 被改成
    恒返回 False,四条拒绝用例照样全过)。

    ★ 变异:把 `cred_matches` 结尾的 `return cred.token_sha256 == ident.token_sha256`
      改成 `return False` → 本条红(而四条拒绝用例仍绿)。
    """
    at = L.now_ms()
    cred, ident = _matching_pair(at)
    assert L.cred_matches(cred, ident, at) is True


def test_cred_mismatch_on_instance_uid_is_rejected() -> None:
    """★ 四元组之 ①instance_uid:换了 DS 实例就不是同一份凭据。

    同名 Pod 重建后 GameServer UID 会变。不比 uid 的话,旧实例的凭据能给新实例的
    心跳背书 —— §9.22 要求的 exact 实例绑定当场失效,而两边日志全绿。

    ★ 变异:删掉 `cred_matches` 里的 `cred.instance_uid != ident.instance_uid` → 本条红。
    """
    at = L.now_ms()
    cred, ident = _matching_pair(at)
    other = L.CredentialIdentity(
        gen=ident.gen,
        jti=ident.jti,
        instance_uid="uid-REBUILT",
        protocol_epoch=ident.protocol_epoch,
        token_sha256=ident.token_sha256,
        kid=ident.kid,
        writer_epoch=ident.writer_epoch,
    )
    assert L.cred_matches(cred, other, at) is False


def test_cred_mismatch_on_protocol_epoch_is_rejected() -> None:
    """★ 四元组之 ②protocol_epoch:实例轮次不同就不是同一次 bootstrap。

    epoch 是抗"代际计数器因 TTL 复位而回退"的那一层。只比 gen+jti 时,一次复位
    就能让旧 gen 重新变成"当前代际",迟到的旧心跳因此复活。

    ★ 变异:删掉 `cred_matches` 里的 `cred.protocol_epoch != ident.protocol_epoch` → 本条红。
    """
    at = L.now_ms()
    cred, ident = _matching_pair(at)
    other = L.CredentialIdentity(
        gen=ident.gen,
        jti=ident.jti,
        instance_uid=ident.instance_uid,
        protocol_epoch=ident.protocol_epoch + 1,
        token_sha256=ident.token_sha256,
        kid=ident.kid,
        writer_epoch=ident.writer_epoch,
    )
    assert L.cred_matches(cred, other, at) is False


def test_cred_mismatch_on_gen_is_rejected() -> None:
    """★ 四元组之 ③gen:代际是单调门,低代际令牌永远不能顶掉高代际。

    ★ 变异:删掉 `cred_matches` 里的 `cred.gen != ident.gen` → 本条红。
    """
    at = L.now_ms()
    cred, ident = _matching_pair(at)
    other = L.CredentialIdentity(
        gen=ident.gen - 1,
        jti=ident.jti,
        instance_uid=ident.instance_uid,
        protocol_epoch=ident.protocol_epoch,
        token_sha256=ident.token_sha256,
        kid=ident.kid,
        writer_epoch=ident.writer_epoch,
    )
    assert L.cred_matches(cred, other, at) is False


def test_cred_mismatch_on_jti_is_rejected() -> None:
    """★ 四元组之 ④jti:同 gen 也可能是两张不同的令牌。

    gen 由 Redis INCR 发号,但计数器复位 / 并发重签都可能让同一个 gen 出现两次。
    jti 是那一刻唯一区分它们的东西;不比 jti = 承认"gen 相同即同一张票"。

    ★ 变异:删掉 `cred_matches` 里的 `cred.jti != ident.jti` → 本条红。
    """
    at = L.now_ms()
    cred, ident = _matching_pair(at)
    other = L.CredentialIdentity(
        gen=ident.gen,
        jti="j7-OTHER",
        instance_uid=ident.instance_uid,
        protocol_epoch=ident.protocol_epoch,
        token_sha256=ident.token_sha256,
        kid=ident.kid,
        writer_epoch=ident.writer_epoch,
    )
    assert L.cred_matches(cred, other, at) is False


def test_cred_matches_never_degrades_when_hash_is_absent() -> None:
    """★ 缺 token hash 时必须 fail-closed,而不是"降级成只比 gen+jti"。

    存储侧缺 hash(旧记录)与调用方缺 hash(没解出 JWT)都算缺 —— 两边各测一次。

    ★ 变异:把 `cred_matches` 最后一行改成
      `return ident.token_sha256 == "" or cred.token_sha256 == ident.token_sha256` → 本条红。
    """
    at = L.now_ms()
    cred, ident = _matching_pair(at)
    caller_without_hash = L.CredentialIdentity(
        gen=ident.gen,
        jti=ident.jti,
        instance_uid=ident.instance_uid,
        protocol_epoch=ident.protocol_epoch,
        token_sha256="",
        kid=ident.kid,
        writer_epoch=ident.writer_epoch,
    )
    assert L.cred_matches(cred, caller_without_hash, at) is False

    stored_without_hash = _cred(uid="uid-a", epoch=3, gen=7, jti="j7", at_ms=at)
    stored_without_hash.token_sha256 = ""
    assert L.cred_matches(stored_without_hash, ident, at) is False


def test_stored_credential_with_legacy_writer_epoch_is_auth_stale() -> None:
    """★ writer_epoch 必须**恰等于** V2,低代际 writer 一律 AuthStale。

    这是 Model B 的机械激活栅栏:放行低代际 = 承认一个不该再有写权的 DS 还能记账。
    注意它抛的是 `AuthStaleError`(ErrUnauthorized),不是"字段不全"的 ErrInvalidArg ——
    两者对调用方的处置完全不同(重新走 bootstrap vs 修请求)。

    ★ 变异:把 `validate_stored_credential` 里的 `!= DS_AUTH_WRITER_EPOCH_V2`
      改成 `< DS_AUTH_WRITER_EPOCH_V2` → 本条红。
    """
    at = L.now_ms()
    cred = _cred(uid="uid-a", epoch=1, gen=1, jti="j1", at_ms=at)
    cred.writer_epoch = WRITER - 1
    with pytest.raises(L.AuthStaleError) as got:
        L.validate_stored_credential(cred, at)
    assert got.value.code == errcode.ErrUnauthorized


def test_phase_locked_uses_generated_enum_not_hand_copied_numbers() -> None:
    """★ 已隔离 / 下线中的相位必须锁死,且判据取自生成物枚举。

    手抄常量在 proto 改动后**不会报错**,只会让"已吊销"被判成"未知相位" ——
    一台被隔离的 DS 重新可分配。这里顺带用 `enum_name` 把相位名写进断言消息,
    未知值也不会把断言本身炸掉(protoenum 模块头)。

    ★ 变异:把 `phase_locked` 的返回改成只判 `HUB_AUTH_PHASE_QUARANTINED` → 本条红。
    """
    for phase in (hubpb.HUB_AUTH_PHASE_QUARANTINED, hubpb.HUB_AUTH_PHASE_TERMINATING):
        assert L.phase_locked(phase), enum_name(hubpb.HubAuthPhase, phase)
    for phase in (
        hubpb.HUB_AUTH_PHASE_BOOTSTRAP,
        hubpb.HUB_AUTH_PHASE_ACTIVE,
        hubpb.HUB_AUTH_PHASE_ROTATING,
    ):
        assert not L.phase_locked(phase), enum_name(hubpb.HubAuthPhase, phase)
    assert L.phase_serving(hubpb.HUB_AUTH_PHASE_ACTIVE)
    assert L.phase_serving(hubpb.HUB_AUTH_PHASE_ROTATING)
    assert not L.phase_serving(hubpb.HUB_AUTH_PHASE_BOOTSTRAP)


# ── ③ successor capability:编码即身份 ───────────────────────────────────────


def test_successor_capability_roundtrips_the_whole_tuple() -> None:
    """★ capability 编的是**整个元组**,不是哈希 —— 解出来必须逐字段还原。

    只存哈希就只能验"等不等",验不出"是什么";而 loader 需要用它证明
    「HASH field、protobuf value、admission 请求」描述的是同一个 placement 操作。

    ★ 变异:把 `successor_capability` 的 canonical 拼串里 `str(ident.source_match_id)`
      改成 `"0"` → 本条红(解回来的 source_match_id 丢了)。
    """
    op = str(uuid.uuid4())
    ident = L.ReservationIdentity(
        player_id=301,
        assignment_id="asg-301",
        instance_uid="uid-a",
        protocol_epoch=2,
        writer_epoch=WRITER,
        placement_version=5,
        placement_operation_id=op,
        source_match_id=999,
    )
    cap = L.successor_capability("t03-a", ident)
    got = L.decode_successor_capability(cap, "t03-a")
    assert got.player_id == 301
    assert got.assignment_id == "asg-301"
    assert got.instance_uid == "uid-a"
    assert got.protocol_epoch == 2
    assert got.writer_epoch == WRITER
    assert got.placement_version == 5
    assert got.placement_operation_id == op
    assert got.source_match_id == 999


def test_successor_capability_rejects_non_canonical_base64() -> None:
    """★ 非 canonical 的 base64 变体必须拒 —— 否则同一个座位会有两条"都合法"的 lease。

    base64 允许补位 bit 非零、允许带 `=` 填充,不同字节串能解出同一个元组。
    不做"重编码后逐字节相等"的回环校验,同 assignment 就会出现两条 successor:
    重复计容,且怎么清都清不干净(其中一条永远匹配不上 canonical field)。

    ★ 变异:删掉 `decode_successor_capability` 结尾的 `if encoded != capability: raise` → 本条红。

    ★ 为什么遍历不同**长度**的 assignment_id 而不是写死一个:canonical 串长度 mod 3
      决定了"存不存在非 canonical 变体"。长度恰好让 base64 无填充位、末字符也没有
      未使用 bit 时,变体根本不存在。先前写死一个 id 的版本恰好撞上这种长度,
      构造出的"变体"与原串逐字节相等,测的是寂寞;改成只遍历 player_id 也没用 ——
      同为 3 位数时长度不变,mod 3 恒定,12 个 id 全都造不出变体。
    """
    variants_tested = 0
    for extra in range(3):  # 三种长度覆盖 mod 3 的全部余数,保证两类变体都出现
        ident = L.ReservationIdentity(
            player_id=302,
            assignment_id="asg-302" + "x" * extra,
            instance_uid="uid-a",
            protocol_epoch=1,
            writer_epoch=WRITER,
        )
        cap = L.successor_capability("t03-b", ident)
        for variant in _non_canonical_variants(cap):
            assert variant != cap, "构造出的变体必须真的不同于 canonical 串"
            variants_tested += 1
            with pytest.raises(errcode.PandoraError) as got:
                L.decode_successor_capability(variant, "t03-b")
            assert got.value.code == errcode.ErrInvalidState
    assert variants_tested >= 2, "没造出任何非 canonical 变体 = 本条没测到东西"


def test_successor_capability_rejects_newline_and_wrong_pod() -> None:
    """★ 编码前拒换行,解码时钉死 pod。

    canonical 形式用 `\\n` 分段,所以一个含 `\\n` 的 assignment_id 能拼出别人的
    capability(段错位),等于伪造一张别人座位的接力票。
    pod 不符则是"拿 A 机的票去 B 机消费",必须在解码这一层就断掉。

    ★ 变异:删掉 `successor_capability` 里的 `any(ch in ("\\r", "\\n") ...)` 判据 → 本条红。
    """
    forged = L.ReservationIdentity(
        player_id=303,
        assignment_id="asg\n303",
        instance_uid="uid-a",
        protocol_epoch=1,
        writer_epoch=WRITER,
    )
    with pytest.raises(errcode.PandoraError) as got:
        L.successor_capability("t03-c", forged)
    assert got.value.code == errcode.ErrInvalidArg

    ok = L.ReservationIdentity(
        player_id=304,
        assignment_id="asg-304",
        instance_uid="uid-a",
        protocol_epoch=1,
        writer_epoch=WRITER,
    )
    cap = L.successor_capability("t03-c", ok)
    with pytest.raises(errcode.PandoraError):
        L.decode_successor_capability(cap, "t03-OTHER")


def test_half_lineage_placement_is_rejected_as_corrupt() -> None:
    """★ `placement_version == 0` 时三个 placement 字段必须**同时**为空。

    半截 lineage 不是"旧记录",是坏数据:version 为 0 却带着 operation_id,说明
    某条路径只写了一半。放行它 = 让一次 placement 操作的接力票能被另一次消费。

    ★ 变异:把 `reservation_placement_valid` 的 version==0 分支改成 `return True` → 本条红。
    """
    half = L.ReservationIdentity(
        player_id=305,
        assignment_id="asg-305",
        instance_uid="uid-a",
        protocol_epoch=1,
        writer_epoch=WRITER,
        placement_version=0,
        placement_operation_id=str(uuid.uuid4()),
    )
    assert L.reservation_placement_valid(half) is False
    versioned_without_uuid = L.ReservationIdentity(
        player_id=305,
        assignment_id="asg-305",
        instance_uid="uid-a",
        protocol_epoch=1,
        writer_epoch=WRITER,
        placement_version=1,
        placement_operation_id="not-a-uuid",
    )
    assert L.reservation_placement_valid(versioned_without_uuid) is False


def test_record_match_refuses_empty_caller_identity() -> None:
    """★ 空身份不得与"同样是空"的旧格式记录互相匹配上。

    这条防的不是"记录脏",是"调用方拿着空身份来问":如果只做
    `rec.uid == ident.uid`,一个还没解析出 UID 的调用方(空串)会匹配上任意一条
    旧格式残留记录 —— 幽灵占座从此永远清不掉。

    ★ 变异:删掉 `reservation_record_matches` 里的 `ident.instance_uid != ""` → 本条红。
    """
    rec = _res_rec(306, "asg-306", pod="t03-d", uid="", expires_at_ms=0)
    rec.auth_epoch = 0
    empty = L.ReservationIdentity(
        player_id=306,
        assignment_id="asg-306",
        instance_uid="",
        protocol_epoch=0,
        writer_epoch=WRITER,
    )
    assert L.reservation_record_matches(rec, "t03-d", empty) is False


# ── ④ 三条容量不变量(§9.22 的账本表达)─────────────────────────────────────


def test_connected_ownership_never_expires_by_time() -> None:
    """★ 已连接归属**没有时间 TTL** —— 时间推进一百年也不能把它删掉。

    删掉它就是"假装玩家已经离开",而他可能还在那台 DS 上打。connected ownership
    只由 exact Departure 或已确认的 UID teardown 删除(§9.22)。

    对照组:同一本账本里的 reservation 与 successor **必须**按时间过期;
    没有对照组的话,一个"prune 整段被注释掉"的改动会让本条假绿。

    ★ 变异:把 `capacity.prune` 里 sessions 分支的
      `rec.expires_at_ms > 0 and rec.expires_at_ms <= now_ms` 前置条件删掉
      (只留 `rec.expires_at_ms <= now_ms`)→ 本条红。
    """
    pod, uid = "t04-a", "uid-a"
    at = L.now_ms()
    ledger = L.HubCapacityLedger(
        reservations={"asg-r": _res_rec(401, "asg-r", pod=pod, uid=uid, expires_at_ms=at + 60_000)},
        sessions={"asg-s": _session_rec(402, "asg-s", pod=pod, uid=uid)},
    )
    cap = L.successor_capability(
        pod,
        L.ReservationIdentity(
            player_id=403,
            assignment_id="asg-x",
            instance_uid=uid,
            protocol_epoch=1,
            writer_epoch=WRITER,
        ),
    )
    ledger.successors[cap] = _res_rec(403, "asg-x", pod=pod, uid=uid, expires_at_ms=at + 60_000)

    far_future = at + 100 * 365 * 24 * 3_600_000  # 一百年后
    L.prune_ledger(ledger, pod=pod, uid=uid, epoch=1, writer=WRITER, at_ms=far_future)

    assert list(ledger.sessions) == ["asg-s"], "已连接归属被时间清掉了 = 假装玩家已离场"
    assert ledger.reservations == {}, "reservation 反而没按时间过期 = prune 被整段架空"
    assert ledger.successors == {}, "successor 是有界接力,必须按时间过期"


def test_player_count_derives_from_ledger_union_not_heartbeat_report() -> None:
    """★ 心跳上报的人数与账本并集**不一致时,以账本为准**。

    心跳是 DS 报的,而 DS 可能漏报、可能在网络分区里报旧数据。拿它覆盖账本 =
    让一台失联的 DS 决定服务端认为它上面有几个人:少报会让服务端超额分配座位,
    多报会让 Hub 永远满员。

    这里刻意让两者**严重分叉**(账本 3 人 / 心跳报 57 人),再断言投影仍是 3。
    `apply_heartbeat_audit` 只把差值交出去做告警,一个容量字段都不许动。

    ★ 变异:在 `capacity.sync_shard_projection` 末尾加一行
      `shard.player_count = reported`(或让 `apply_heartbeat_audit` 顺手改
      `shard.player_count`)→ 本条红。
    """
    pod, uid = "t04-b", "uid-a"
    at = L.now_ms()
    ledger = L.HubCapacityLedger(
        reservations={
            "asg-1": _res_rec(411, "asg-1", pod=pod, uid=uid, expires_at_ms=at + 60_000)
        },
        sessions={
            "asg-2": _session_rec(412, "asg-2", pod=pod, uid=uid),
            "asg-3": _session_rec(413, "asg-3", pod=pod, uid=uid),
        },
    )
    shard = hubpb.HubShardStorageRecord(hub_pod_name=pod, capacity=500)
    L.sync_shard_capacity_projection(shard, ledger)

    assert (shard.reserved_count, shard.connected_ownership_count) == (1, 2)
    assert shard.player_count == 3

    from pandorapy.services.hub_allocator import capacity as C

    projection = C.ShardProjection(capacity=500, player_count=shard.player_count)
    drift = C.apply_heartbeat_audit(projection, reported_count=57)
    assert drift == 54, "差值必须原样交出去做告警"
    assert projection.player_count == 3, "心跳只写审计字段,不许改容量"


def test_successor_is_a_bounded_relay_and_never_double_counts() -> None:
    """★ successor 在旧 owner 还活着时**不计容**,旧 owner 走后才顶上那一格。

    不去重的话,一个玩家在重连的瞬间会同时占「已连接」和「接力预留」两格 ——
    重连高峰期 Hub 会假性满员,而账本里每一条记录看起来都合法。

    三段断言依次对应"重签中 / 旧 owner 离场后 / 全新玩家":
      ① session + successor 同 assignment → 只算 1 格;
      ② 删掉 session 后 successor 立刻变成那 1 格 reserved;
      ③ 另一个 assignment 的 successor 是**另一个人**,必须照常计容。

    ★ 变异:把 `capacity.counts` 里的
      `if successor.assignment_id not in ledger.sessions:` 条件删掉(无条件 add)→ 本条红。
    """
    pod, uid = "t04-c", "uid-a"
    at = L.now_ms()
    same = L.ReservationIdentity(
        player_id=421,
        assignment_id="asg-421",
        instance_uid=uid,
        protocol_epoch=1,
        writer_epoch=WRITER,
        placement_version=1,
        placement_operation_id=str(uuid.uuid4()),
    )
    cap_same = L.successor_capability(pod, same)
    ledger = L.HubCapacityLedger(
        sessions={"asg-421": _session_rec(421, "asg-421", pod=pod, uid=uid)},
        successors={
            cap_same: _res_rec(421, "asg-421", pod=pod, uid=uid, expires_at_ms=at + 60_000)
        },
    )
    assert L.ledger_counts(ledger, 500) == (0, 1), "① 旧 owner 还在,successor 不得计容"

    del ledger.sessions["asg-421"]
    assert L.ledger_counts(ledger, 500) == (1, 0), "② 旧 owner 离场,successor 顶上那一格"

    other = L.ReservationIdentity(
        player_id=422,
        assignment_id="asg-422",
        instance_uid=uid,
        protocol_epoch=1,
        writer_epoch=WRITER,
    )
    ledger.successors[L.successor_capability(pod, other)] = _res_rec(
        422, "asg-422", pod=pod, uid=uid, expires_at_ms=at + 60_000
    )
    assert L.ledger_counts(ledger, 500) == (2, 0), "③ 不同 assignment 是不同的人,要各占一格"


def test_ledger_counts_rejects_a_self_contradictory_book() -> None:
    """★ 账本自身不自洽时**整条拒**,而不是"挑掉坏的继续算"。

    同一个 assignment 同时出现在 reservation 与 session 里 = 状态机漏了一次转移;
    此时任何派生值都不可信,继续算只会把错误写回 Redis。

    ★ 变异:删掉 `capacity.counts` 里 `for aid in ledger.reservations: if aid in
      ledger.sessions: raise` 这段 → 本条红。
    """
    pod, uid = "t04-d", "uid-a"
    at = L.now_ms()
    ledger = L.HubCapacityLedger(
        reservations={
            "asg-431": _res_rec(431, "asg-431", pod=pod, uid=uid, expires_at_ms=at + 60_000)
        },
        sessions={"asg-431": _session_rec(431, "asg-431", pod=pod, uid=uid)},
    )
    with pytest.raises(errcode.PandoraError) as got:
        L.ledger_counts(ledger, 500)
    assert got.value.code == errcode.ErrInvalidState


def test_prune_drops_records_of_a_rebuilt_instance() -> None:
    """★ uid / epoch 换了以后,旧实例的账本记录一条都不能留。

    同名 Pod 重建后 UID 会变。不比对就等于把旧实例的座位算进新实例的容量 ——
    幽灵占座,Hub 明明是空的却分不进人。

    ★ 变异:把 `capacity.record_matches_instance` 的
      `rec.hub_instance_uid == uid` 去掉 → 本条红。
    """
    pod = "t04-e"
    at = L.now_ms()
    ledger = L.HubCapacityLedger(
        reservations={
            "asg-441": _res_rec(441, "asg-441", pod=pod, uid="uid-OLD", expires_at_ms=at + 60_000)
        },
        sessions={"asg-442": _session_rec(442, "asg-442", pod=pod, uid="uid-OLD")},
    )
    L.prune_ledger(ledger, pod=pod, uid="uid-NEW", epoch=1, writer=WRITER, at_ms=at)
    assert ledger.reservations == {}
    assert ledger.sessions == {}


# ── ⑤ 写者继任 fencing(解包顺序 = Python 侧,不是 Go 侧)──────────────────────


async def test_lost_writer_lease_fails_closed_with_zero_writes(rdb) -> None:
    """★ 失去写者租约的副本必须零写入,而不是"照样记账"。

    这条同时钉死 `guard_writer_fence` 的解包顺序:Python 的
    `writerlease.Lease.current()` 返回 `(held, token)`,与 Go 的 `(token, held)`
    **相反**(writer_fence.py 模块头专门写了这条)。抄成 Go 顺序时,
    `held=False, token=9` 会被解成 `mine=False(=0), held=9(真值)` —— 失主的旧写者
    照样放行,fencing 静默失效且没有任何运行期信号。

    ★ 变异:把 `guard_writer_fence` 里的 `held, mine = fence.current()` 改回
      `mine, held = fence.current()` → 本条红(reserve 不再抛 WriterSuperseded)。
      这正是移植时的真实缺陷,已随本轮修复。
    """
    pod = "t05-a"
    _, _, at = await _bootstrap(rdb, pod)
    ops = L.HubCapacityLedgerOps(rdb, FakeLease(token=9, held=False))
    with pytest.raises(L.WriterSupersededError) as got:
        await ops.reserve_assignment(pod, _reservation(500001, "asg-500001", uid="uid-a", at_ms=at), at, 0, TTL)
    assert got.value.code == errcode.ErrUnavailable
    assert await rdb.hlen(L.reservations_key(pod)) == 0


async def test_writer_watermark_records_the_token_not_a_boolean(rdb) -> None:
    """★ 水位键里落的必须是**单调 token**,不是被解错位的 True/False。

    水位是"继任者已经触达此 slot"的唯一证据,且刻意持久(SET 不带 TTL)。
    写成 1/0 的话所有历届写者的水位都挤在 {0,1} 两个值上,`cur > mine` 恒不成立,
    迟到的旧写者可以一直写下去。

    ★ 变异:同上一条(解包顺序反了 → 水位写成 "1" 而不是 "7")→ 本条红。
    """
    pod = "t05-b"
    _, _, at = await _bootstrap(rdb, pod)
    ops = L.HubCapacityLedgerOps(rdb, FakeLease(token=7))
    got = await ops.reserve_assignment(
        pod, _reservation(500101, "asg-500101", uid="uid-a", at_ms=at), at, 0, TTL
    )
    assert got.ok is True
    assert await rdb.get(L.wfence_key(pod)) == b"7"
    assert await rdb.pttl(L.wfence_key(pod)) == -1, "fencing 水位必须比业务记录长寿(无 TTL)"

    superseded = L.HubCapacityLedgerOps(rdb, FakeLease(token=6))
    with pytest.raises(L.WriterSupersededError):
        await superseded.reserve_assignment(
            pod, _reservation(500102, "asg-500102", uid="uid-a", at_ms=at), at, 0, TTL
        )
    assert await rdb.hlen(L.reservations_key(pod)) == 1, "被继任的旧写者不得留下任何记录"


# ── ⑥ 账本落盘:session 不进到期 ZSET ────────────────────────────────────────


async def test_admission_writes_no_session_expiry_entry(rdb) -> None:
    """★ 已连接归属**不写到期 ZSET** —— 写进去就等于给它安了一个假的到期时刻。

    §9.22 的"没有时间 TTL"不只是 prune 那一处判据:只要 sessions 的 expiry ZSET
    里有条目,任何一条按 ZSET 扫描做清理的路径(现有的或以后加的)都会把一个
    还在场的玩家当成过期座位回收掉。所以它必须在**落盘那一步**就不存在。

    对照组:reservation / successor 的到期 ZSET 必须有条目。

    ★ 变异:把 `write_hub_capacity_ledger` 里 session 分支的
      `if rec.expires_at_ms > 0:` 去掉(无条件 zadd)→ 本条红。
    """
    pod, uid = "t06-a", "uid-a"
    _, _, at = await _bootstrap(rdb, pod)
    ops = L.HubCapacityLedgerOps(rdb)

    reservation = _reservation(600001, "asg-600001", uid=uid, at_ms=at)
    assert (await ops.reserve_assignment(pod, reservation, at, 0, TTL)).ok
    assert await rdb.zcard(L.reservation_expiry_key(pod)) == 1

    admission_id = str(uuid.uuid4())
    _, cred, _ = None, None, None  # noqa: F841 —— 占位,凭据由下方重新取
    repo = hauth.RedisHubAuthRepo(rdb)
    rec, found = await repo.get_auth(pod)
    assert found and rec is not None
    out = await ops.acknowledge_admission(
        pod, _ident(rec.active), reservation, admission_id, 1, at, TTL
    )
    assert out.admitted is True

    assert await rdb.hlen(L.sessions_key(pod)) == 1
    assert await rdb.zcard(L.session_expiry_key(pod)) == 0, "已连接归属不得进到期 ZSET"
    assert await rdb.hlen(L.reservations_key(pod)) == 0, "reservation 必须被原子消费掉"
    assert await rdb.zcard(L.reservation_expiry_key(pod)) == 0


async def test_ledger_key_retention_outlives_the_latest_lease(rdb) -> None:
    """★ 整键 retention 取「最晚单项绝对到期 + guard」,不能沿用较短的 shard TTL。

    沿用 shard TTL 会提前删掉仍然有效的 lease:玩家拿着合法票据过来,座位却没了 ——
    表现是"进大厅失败"而全链日志绿的。

    ★ 变异:把 `write_hub_capacity_ledger` 里的
      `pexpireat(..., latest + CAPACITY_LEDGER_RETENTION_GUARD_MS)` 改成用
      `shard_ttl` 之类的短值(或把 guard 改成 0)→ 本条红。
    """
    pod, uid = "t06-b", "uid-a"
    _, _, at = await _bootstrap(rdb, pod)
    ops = L.HubCapacityLedgerOps(rdb)
    reservation = _reservation(600101, "asg-600101", uid=uid, at_ms=at)
    assert (await ops.reserve_assignment(pod, reservation, at, 0, TTL)).ok

    pttl = await rdb.pttl(L.reservations_key(pod))
    # ★ 基准是 `expires_at_ms`(本键里各项自己的到期,也是到期 ZSET 的 score),
    #   不是 `assignment_expires_at_ms`。后者是 assignment 本体的 TTL,存在分片镜像
    #   那边,与本键的保留期无关 —— 拿它当基准会把一条忠实于 Go 的实现误判成 bug。
    #   Go 侧 writeHubCapacityLedger 同样取 rec.GetExpiresAtMs()。
    latest = reservation.expires_at_ms
    assert pttl > latest - L.now_ms(), "整键 retention 必须晚于最晚单项绝对到期"
    assert pttl <= latest - L.now_ms() + L.CAPACITY_LEDGER_RETENTION_GUARD_MS + 5_000


# ── ⑦ 端到端:预留 → 准入 → 重签接力 → 离场 ─────────────────────────────────


async def test_seat_lifecycle_never_double_counts_across_reconnect(rdb) -> None:
    """★ 一次重连的完整生命周期里,同一个玩家**始终只占一格**。

    这条把 ④ 的纯函数结论放到真事务上再验一遍 —— 因为中间任何一步多写 / 少删一条
    记录,纯函数层都看不出来:

      ① reserve   → reserved=1 connected=0
      ② admission → reserved=0 connected=1(reservation 被原子消费)
      ③ 重签      → 生成 successor,占用**不变**(玩家还在旧连接上)
      ④ departure → session 删掉,successor 顶成那一格 reserved

    ★ 变异:把 `_reserve_mutate` 里 session 分支结尾的 `return ""` 改成
      "顺手也建一条 reservation" → ③ 的断言变红(占用从 1 变 2)。
    """
    pod, uid = "t07-a", "uid-a"
    _, _, at = await _bootstrap(rdb, pod)
    ops = L.HubCapacityLedgerOps(rdb)
    repo = hauth.RedisHubAuthRepo(rdb)
    rec, _ = await repo.get_auth(pod)
    credential = _ident(rec.active)

    first = _reservation(700001, "asg-700001", uid=uid, at_ms=at)
    assert (await ops.reserve_assignment(pod, first, at, 0, TTL)).ok
    shard = await _shard_of(rdb, pod)
    assert (shard.reserved_count, shard.connected_ownership_count, shard.player_count) == (1, 0, 1)

    admission_id = str(uuid.uuid4())
    admitted = await ops.acknowledge_admission(pod, credential, first, admission_id, 1, at, TTL)
    assert (admitted.reserved_count, admitted.connected_count, admitted.capacity_occupancy) == (
        0,
        1,
        1,
    )

    resign = _reservation(
        700001,
        "asg-700001",
        uid=uid,
        at_ms=at,
        version=1,
        operation_id=str(uuid.uuid4()),
    )
    assert (await ops.reserve_assignment(pod, resign, at, 0, TTL)).ok
    shard = await _shard_of(rdb, pod)
    assert await rdb.hlen(L.successors_key(pod)) == 1
    assert shard.player_count == 1, "重签期间同一个玩家不得同时占两格"
    assert (shard.reserved_count, shard.connected_ownership_count) == (0, 1)

    departed = await ops.acknowledge_departure(
        pod, credential, resign, admission_id, 1, at, TTL
    )
    assert departed.departed is True and departed.conflict is False
    shard = await _shard_of(rdb, pod)
    assert (shard.reserved_count, shard.connected_ownership_count, shard.player_count) == (1, 0, 1)
    assert await rdb.hlen(L.successors_key(pod)) == 1, "接力票要留到新 Admission 消费"


async def test_late_departure_of_a_replaced_connection_is_a_conflict(rdb) -> None:
    """★ 迟到的旧 Logout 必须**零副作用**地冲突返回,不能踢掉新连接。

    重连后 owner 已经换成新的 admission_id/seq。旧连接的 Logout 晚到时,如果按
    assignment 删 session,玩家会在新 DS 上被凭空踢下线(§9.19「不卡玩家」的反面:
    卡不卡先不说,人直接没了)。

    ★ 变异:把 `_departure_mutate` 里
      `if current.admission_seq != admission_seq or current.admission_id != admission_id:`
      改成只判 `admission_seq` → 本条红。
    """
    pod, uid = "t07-b", "uid-a"
    _, _, at = await _bootstrap(rdb, pod)
    ops = L.HubCapacityLedgerOps(rdb)
    repo = hauth.RedisHubAuthRepo(rdb)
    rec, _ = await repo.get_auth(pod)
    credential = _ident(rec.active)

    reservation = _reservation(700101, "asg-700101", uid=uid, at_ms=at)
    assert (await ops.reserve_assignment(pod, reservation, at, 0, TTL)).ok
    live_admission = str(uuid.uuid4())
    assert (
        await ops.acknowledge_admission(pod, credential, reservation, live_admission, 2, at, TTL)
    ).admitted

    stale = await ops.acknowledge_departure(
        pod, credential, reservation, str(uuid.uuid4()), 1, at, TTL
    )
    assert stale.conflict is True
    assert stale.departed is False
    assert await rdb.hlen(L.sessions_key(pod)) == 1, "迟到的旧 Logout 不得删掉新 owner"


async def test_release_seat_demands_physical_departure_proof(rdb) -> None:
    """★ 清账本**不是**物理驱逐证明:活着的 connected owner 只能要求 Departure。

    这是 §9.22 的核心分工 —— Release / Transfer 只能下发物理 eviction 并等待
    那个 proof。允许直接删 session 就等于服务端单方面宣布"玩家已经走了",
    而他可能还在旧 DS 上操作,于是第二台 DS 被放行 = 脑裂。

    结果用**四态**而不是 bool:durable cleanup worker 对"已经不在了"与
    "另一个 owner 占着"的处置完全相反(推进 vs fail-closed)。

    ★ 变异:把 `_release_connected` 里设 `departure_required=True` 的分支改成
      直接删 session 并返回 `released=True` → 本条红。
    """
    pod, uid = "t07-c", "uid-a"
    _, _, at = await _bootstrap(rdb, pod)
    ops = L.HubCapacityLedgerOps(rdb)
    repo = hauth.RedisHubAuthRepo(rdb)
    rec, _ = await repo.get_auth(pod)
    credential = _ident(rec.active)

    reservation = _reservation(700201, "asg-700201", uid=uid, at_ms=at)
    assert (await ops.reserve_assignment(pod, reservation, at, 0, TTL)).ok
    assert (
        await ops.acknowledge_admission(
            pod, credential, reservation, str(uuid.uuid4()), 1, at, TTL
        )
    ).admitted

    expected = L.AssignmentInstanceIdentity(
        player_id=700201,
        assignment_id="asg-700201",
        instance_uid=uid,
        protocol_epoch=1,
        writer_epoch=WRITER,
    )
    # ★ 必须调 `_exact` 版本:不带后缀的那个是 Go `ReleaseAssignmentSeat` 的布尔封装
    #   (只回 released),四态结果在 `ReleaseAssignmentSeatExact`。两个方法 Go 侧同样并存。
    got = await ops.release_assignment_seat_exact(pod, expected, TTL)
    assert got.departure_required is True
    assert got.released is False and got.already_absent is False and got.conflict is False
    assert await rdb.hlen(L.sessions_key(pod)) == 1, "账本清理不得冒充物理驱逐"


async def test_inspect_seat_is_read_only(rdb) -> None:
    """★ 只读视图必须**零变更** —— 包括不顺手 prune、不刷新 TTL。

    对账路径会高频调它。任何一处"顺手清理"都会让一次只读观测变成写操作,
    而观测者往往拿的是旧快照(§10「状态优先查询唯一权威」)。

    ★ 变异:在 `inspect_assignment_seat` 的事务里加一条 `pipe.multi()` + 写命令
      (哪怕只是 `pipe.expire(...)`)→ 本条红。
    """
    pod, uid = "t07-d", "uid-a"
    _, _, at = await _bootstrap(rdb, pod)
    ops = L.HubCapacityLedgerOps(rdb)
    reservation = _reservation(700301, "asg-700301", uid=uid, at_ms=at)
    assert (await ops.reserve_assignment(pod, reservation, at, 0, TTL)).ok

    before = await rdb.hgetall(L.reservations_key(pod))
    before_ttl = await rdb.pttl(L.reservations_key(pod))
    expected = L.AssignmentInstanceIdentity(
        player_id=700301,
        assignment_id="asg-700301",
        instance_uid=uid,
        protocol_epoch=1,
        writer_epoch=WRITER,
    )
    snapshot = await ops.inspect_assignment_seat(pod, expected)
    assert snapshot.reserved is True
    assert snapshot.connected is False
    assert await rdb.hgetall(L.reservations_key(pod)) == before
    assert abs(await rdb.pttl(L.reservations_key(pod)) - before_ttl) < 3_000


async def test_reserve_is_rejected_once_the_shard_is_full(rdb) -> None:
    """★ 容量闸在**账本并集**上判,满了就拒,不靠心跳数字。

    容量 1 的分片放进第二个玩家,`reason` 必须是 `shard-full` 且账本不变。
    reason 只进日志、不外露客户端(它含内部状态词汇)。

    ★ 变异:把 `_reserve_mutate` 里的 `if reserved + connected >= shard_capacity:`
      改成 `>` → 本条红(容量 1 的分片会被塞进 2 个人)。
    """
    pod, uid = "t07-e", "uid-a"
    _, _, at = await _bootstrap(rdb, pod, capacity=1)
    ops = L.HubCapacityLedgerOps(rdb)
    assert (
        await ops.reserve_assignment(
            pod, _reservation(700401, "asg-700401", uid=uid, at_ms=at), at, 0, TTL
        )
    ).ok
    second = await ops.reserve_assignment(
        pod, _reservation(700402, "asg-700402", uid=uid, at_ms=at), at, 0, TTL
    )
    assert second.ok is False
    assert second.reason == "shard-full"
    assert await rdb.hlen(L.reservations_key(pod)) == 1


async def test_reserve_from_a_stale_instance_is_rejected(rdb) -> None:
    """★ 拿旧实例身份来占座 → `reservation-instance-mismatch`,零写入。

    Pod 重建后 UID 变了,而在途的分配请求还带着旧 UID。放行它 = 在新实例的账本里
    记一笔属于旧实例的座位,prune 又会把它清掉 —— 玩家拿到票却发现座位不存在。

    ★ 变异:删掉 `reserve_assignment` 里
      `reservation.instance_uid != auth_rec.instance_uid` 那条判据 → 本条红。
    """
    pod = "t07-f"
    _, _, at = await _bootstrap(rdb, pod, uid="uid-a")
    ops = L.HubCapacityLedgerOps(rdb)
    got = await ops.reserve_assignment(
        pod, _reservation(700501, "asg-700501", uid="uid-STALE", at_ms=at), at, 0, TTL
    )
    assert got.ok is False
    assert got.reason == "reservation-instance-mismatch"
    assert await rdb.hlen(L.reservations_key(pod)) == 0


# ── ⑧ 拆机证明按不可变 UID 存 ────────────────────────────────────────────────


async def test_teardown_proof_is_keyed_by_immutable_gameserver_uid(rdb) -> None:
    """★ 拆机证明按 GameServer UID 存,不按 pod 名。

    同名 Pod 会被重建。按 pod 名存的话:新实例一上线就"继承"了旧实例的拆机证明,
    于是别人可以拿它来清理**新实例**上活着的玩家归属 —— 直接打穿 §9.22。

    ★ 变异:把 `has_instance_teardown_proof` 的 field 从 `instance_uid` 改成 pod
      (或改成恒真)→ 本条红。
    """
    pod = "t08-a"
    ops = L.HubCapacityLedgerOps(rdb)
    assert await ops.has_instance_teardown_proof(pod, "uid-old") is False
    await ops.record_instance_teardown_proof(pod, "uid-old", 600.0)
    assert await ops.has_instance_teardown_proof(pod, "uid-old") is True
    assert await ops.has_instance_teardown_proof(pod, "uid-new") is False, (
        "同名 Pod 重建后的新 UID 不得继承旧 UID 的拆机证明"
    )
