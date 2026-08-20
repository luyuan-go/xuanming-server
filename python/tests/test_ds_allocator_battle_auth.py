"""Battle DS **Model B 授权权威**(Redis 唯一权威)回归测试 ——
覆盖 `pandorapy/services/ds_allocator/battle_auth.py`。

对应 Go 侧 `services/battle/ds_allocator/internal/data/battle_auth.go`(2631 行);
每条断言的行为口径都以那份 Go 源码为准,不以 Python 实现"看起来该怎样"为准。

## 本文件盯死的五件事(`CLAUDE.md` §9 不变量 1 / 3 / 6 / 22)

  ① **凭据身份是四元组 `(instance_uid, instance_epoch, gen, jti)`,任一位不符即拒**。
     这不是"多写几个字段更保险",而是每一位各挡一种复活:
       - `instance_uid` 挡同名 GameServer 重建(Pod 名一样、UID 换代);
       - `instance_epoch` 挡同 UID 下的重新绑定;
       - `gen` 挡代际重放;
       - `jti` 挡"同代际重签的另一张票"。
     所以四条**各写一个独立用例**:合并成一条参数化,删掉其中任意一格判据仍会绿。
  ② **`high_water_gen` 是单调水位**。stage 要求 `gen > high_water`,心跳要求
     `high_water >= gen`。少了它,`authgen` 计数器一次复位就能让被吊销的旧代际复活。
  ③ **旧 epoch 的迟到写被 fencing 拒绝**,且拒绝路径**零副作用**(不改字节、不刷 TTL)。
  ④ **QUARANTINED / ROTATING 两个相位**:前者是实例级永久墓碑(连"换个 UID 重新
     Prepare"都不许),后者要求新旧凭据在同一记录里并存且**互不串味**。
  ⑤ **读不到 ≠ 读不了**。auth/battle 缺键 → `BattleAuthStaleError`(拒绝);
     Redis 报错 / WRONGTYPE / 连接失败 → **原样冒泡**,绝不退化成"没有凭据"或"校验通过"。

## 依赖策略

用**真 Redis**(默认 `127.0.0.1:16379`),连不上回落 `fakeredis`(**从不 skip**)。
本模块的权威语义就是 `WATCH/MULTI/EXEC` + 单条 Lua 原子读 + `PERSIST`/`PX` 的 TTL
三态,给它留"环境不好就整体不跑"的后门等于把最关键的几条断言变成可选项:

    docker run -d --name pandora-redis -p 16379:6379 redis:8-alpine

★ 每个用例用**独立 match_id**,避免跨用例污染。
★ 每条用例 docstring 的 `★ 变异:` 一行都真跑过(改坏 → 红 → 改回 → 绿),不是照代码猜的。
"""

from __future__ import annotations

import asyncio
import dataclasses
import os
import uuid as _uuid

import pytest
import redis.exceptions as redis_exc
from pandora.ds.v1 import allocator_pb2 as dspb

from pandorapy import errcode
from pandorapy.auth import DS_AUTH_WRITER_EPOCH_V2 as WRITER
from pandorapy.protoenum import enum_name
from pandorapy.services.ds_allocator import battle_auth as BA

# ── 常量 ────────────────────────────────────────────────────────────────────

AUTH_TTL = 600.0
BATTLE_TTL = 900.0

#: 一个远期固定 exp:凭据完整性判据里有 `exp_ms > now`,用 `now+1h` 会让一次跑得慢的
#: CI 把与到期无关的用例判红。
FAR_EXP_MS = 2_000_000_000_000

POD = "pandora-battle-0"
UID_A = "gs-uid-A"
UID_B = "gs-uid-B"
KID = "kid-1"


# ── fixture ─────────────────────────────────────────────────────────────────


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


@pytest.fixture
def repo(rdb) -> BA.RedisBattleAuthRepo:  # noqa: ANN001
    return BA.RedisBattleAuthRepo(rdb)


# ── 构造辅助 ────────────────────────────────────────────────────────────────


def _alloc_id() -> str:
    return str(_uuid.uuid4())


def _binding(match_id: int, allocation_id: str, *, uid: str = UID_A, pod: str = POD):
    return BA.BattleAuthorityBinding(
        match_id=match_id,
        allocation_id=allocation_id,
        pod_name=pod,
        instance_uid=uid,
        required_writer_epoch=WRITER,
        auth_ttl_sec=AUTH_TTL,
        battle_ttl_sec=BATTLE_TTL,
    )


def _cred(seed: BA.BattleCredentialSeed, *, uid: str = UID_A, jti: str | None = None):
    """按 Prepare 领到的 (epoch, gen) 造一份**完整且未过期**的存储凭据。"""
    return dspb.BattleDSCredential(
        gen=seed.gen,
        jti=jti or f"jti-{seed.gen}",
        exp_ms=FAR_EXP_MS,
        kid=KID,
        instance_uid=uid,
        instance_epoch=seed.instance_epoch,
        token_sha256=f"sha-{jti or seed.gen}",
        writer_epoch=WRITER,
    )


def _ident(cred, *, pod: str = POD) -> BA.BattleCredentialIdentity:
    """把存储凭据翻成「DS 心跳携带的已验签身份」。"""
    return BA.BattleCredentialIdentity(
        pod_name=pod,
        instance_uid=cred.instance_uid,
        instance_epoch=cred.instance_epoch,
        gen=cred.gen,
        jti=cred.jti,
        exp_ms=cred.exp_ms,
        kid=cred.kid,
        token_sha256=cred.token_sha256,
        writer_epoch=cred.writer_epoch,
    )


def _hb(
    *,
    state: str = "ready",
    players: int = 1,
    beats: int = 0,
    span_ms: int = 0,
    empty_timeout: float = 0.0,
) -> BA.BattleHeartbeatInput:
    return BA.BattleHeartbeatInput(
        player_count=players,
        state=state,
        auth_ttl_sec=AUTH_TTL,
        battle_ttl_sec=BATTLE_TTL,
        empty_battle_timeout_sec=empty_timeout,
        stability_beats=beats,
        stability_span_ms=span_ms,
    )


async def _seed_warming_battle(rdb, match_id: int, allocation_id: str, *, pod: str = POD) -> None:
    """播种一条 warming battle 镜像(Model B 里由 claim→warming 那半段留下的)。"""
    rec = dspb.BattleStorageRecord(
        match_id=match_id,
        allocation_id=allocation_id,
        state="warming",
        ds_pod_name=pod,
        ds_addr="10.0.0.7:7777",
        release_track="stable",
        allocated_at_ms=BA.now_ms(),
    )
    # preactive 窗口内 battle 键**无 TTL**(见 battle_preactive_authority 注释)。
    await rdb.set(BA.battle_key(match_id), rec.SerializeToString())


async def _auth(rdb, match_id: int) -> dspb.BattleDSAuthStorageRecord:
    raw = await rdb.get(BA.battle_auth_key(match_id))
    assert raw is not None, "auth 键不存在"
    rec = dspb.BattleDSAuthStorageRecord()
    BA.unmarshal_battle_auth(match_id, raw, rec)
    return rec


async def _battle(rdb, match_id: int) -> dspb.BattleStorageRecord:
    raw = await rdb.get(BA.battle_key(match_id))
    assert raw is not None, "battle 键不存在"
    return BA.unmarshal_battle(match_id, raw)


async def _activate(rdb, repo, match_id: int, *, uid: str = UID_A, pod: str = POD, burn: int = 0):
    """走真实链路把一局引导到 ACTIVE:seed → Prepare → Stage → MarkDelivered → 心跳。

    刻意不手搓 Redis 字节:手搓漏填一个投影字段,后面的用例会因为一个与被测点无关的
    reason 绿得莫名其妙。返回 `(allocation_id, cred, ident)`。

    `burn` = 正式取号前**先空跑几次 Prepare**,让最终 gen 落在 `burn+1`。
    只有需要构造"比当前代际更低的 gen"的用例才用得上(gen 从 1 起,没有 0 代)。
    """
    allocation_id = _alloc_id()
    await _seed_warming_battle(rdb, match_id, allocation_id, pod=pod)
    for _ in range(burn):
        await repo.prepare_credential(_binding(match_id, allocation_id, uid=uid, pod=pod))
    seed = await repo.prepare_credential(_binding(match_id, allocation_id, uid=uid, pod=pod))
    cred = _cred(seed, uid=uid)
    await repo.stage_pending(
        BA.BattleStageInput(
            match_id=match_id,
            allocation_id=allocation_id,
            credential=cred,
            auth_ttl_sec=AUTH_TTL,
        )
    )
    await repo.mark_delivered(match_id, allocation_id, cred, "rv-1", AUTH_TTL)
    ident = _ident(cred, pod=pod)
    out = await repo.activate_heartbeat(match_id, ident, _hb(state="ready"))
    assert out.first_activation, "引导失败:后续用例的前提不成立"
    return allocation_id, cred, ident


class _AuthSnapshot:
    """auth 键的字节 + TTL 快照,用于断言"拒绝路径零副作用"。"""

    def __init__(self, payload: bytes, pttl: int) -> None:
        self.payload = payload
        self.pttl = pttl


async def _snapshot_auth(rdb, match_id: int, *, pin_pttl_ms: int = 5_000) -> _AuthSnapshot:
    """先把 auth 键 TTL 钉到一个很小的值,再快照。

    为什么要先钉:正常 TTL 是 600s,一次"顺手刷新"在几毫秒的窗口里看不出差别。
    钉到 5s 后,任何刷新都会把 pttl 弹回 ~600000,断言因此是**判定性**的而不是靠时序。
    """
    key = BA.battle_auth_key(match_id)
    await rdb.pexpire(key, pin_pttl_ms)
    return _AuthSnapshot(await rdb.get(key), await rdb.pttl(key))


async def _assert_auth_untouched(rdb, match_id: int, snap: _AuthSnapshot) -> None:
    key = BA.battle_auth_key(match_id)
    assert await rdb.get(key) == snap.payload, "拒绝路径改写了 auth 记录字节"
    now_pttl = await rdb.pttl(key)
    assert now_pttl <= snap.pttl, f"拒绝路径刷了 auth TTL: {snap.pttl} → {now_pttl}"


# ══ ① 凭据四元组:任一位不符即拒(四条独立用例)══════════════════════════════


async def test_heartbeat_rejects_a_credential_whose_instance_uid_differs(rdb, repo) -> None:
    """★ 四元组第 1 位:`instance_uid` 不符 → `authority_binding_mismatch`,零副作用。

    这一位挡的是**同名 GameServer 重建**:Agones 把 Pod 删了重建,名字一模一样、
    UID 换代。只比 pod 名的话,旧实例(还没死透、还在心跳)会被当成新实例接着授权,
    于是同一 match 上出现两个"合法"的 DS —— §9 不变量 1 被直接打穿。

    ★ 变异:删掉 `activate_heartbeat` 里 `or auth.instance_uid != ident.instance_uid`
      → 本条红(会一路走到 NO_USABLE_CREDENTIAL 之外甚至被接受)。
    """
    match_id = 9_101
    _, _, ident = await _activate(rdb, repo, match_id)
    snap = await _snapshot_auth(rdb, match_id)

    bad = dataclasses.replace(ident, instance_uid=UID_B)
    with pytest.raises(BA.BattleAuthStaleError) as got:
        await repo.activate_heartbeat(match_id, bad, _hb(state="running"))
    assert got.value.code == errcode.ErrUnauthorized
    assert got.value.reason == BA.AUTH_REJECT_BINDING_MISMATCH
    await _assert_auth_untouched(rdb, match_id, snap)


async def test_heartbeat_rejects_a_credential_whose_instance_epoch_differs(rdb, repo) -> None:
    """★ 四元组第 2 位:`instance_epoch` 不符 → `authority_binding_mismatch`,零副作用。

    UID 相同但 epoch 不同 = 同一台机器上的**上一轮绑定**。Prepare 换实例时 epoch++,
    旧凭据的 epoch 因此永远追不上。少了这一位,一次"换分配但 UID 复用"就能让旧票复活。

    ★ 变异:删掉 `activate_heartbeat` 里 `or auth.instance_epoch != ident.instance_epoch`
      → 本条红。
    """
    match_id = 9_102
    _, _, ident = await _activate(rdb, repo, match_id)
    snap = await _snapshot_auth(rdb, match_id)

    bad = dataclasses.replace(ident, instance_epoch=ident.instance_epoch + 1)
    with pytest.raises(BA.BattleAuthStaleError) as got:
        await repo.activate_heartbeat(match_id, bad, _hb(state="running"))
    assert got.value.code == errcode.ErrUnauthorized
    assert got.value.reason == BA.AUTH_REJECT_BINDING_MISMATCH
    await _assert_auth_untouched(rdb, match_id, snap)


async def test_heartbeat_rejects_a_credential_whose_gen_differs(rdb, repo) -> None:
    """★ 四元组第 3 位:`gen` 不符 → `no_usable_credential`,零副作用。

    注意这里刻意取 `gen-1`(**低于**水位),把它与"高于水位"那条规则分开:
    低 gen 走不到 `gen_below_high_water`,只可能被凭据全等比对拦住。两条规则各测各的,
    否则删掉任意一条用例仍绿。

    ★ 变异:把 `battle_credential_matches` 里的 `and cred.gen == ident.gen` 删掉
      → 本条红(低代际凭据被当成 active 接受)。
    """
    match_id = 9_103
    _, _, ident = await _activate(rdb, repo, match_id, burn=1)
    assert ident.gen == 2, "构造前提:必须有一个比当前代际更低的合法号可用"
    snap = await _snapshot_auth(rdb, match_id)

    bad = dataclasses.replace(ident, gen=ident.gen - 1)
    with pytest.raises(BA.BattleAuthStaleError) as got:
        await repo.activate_heartbeat(match_id, bad, _hb(state="running"))
    assert got.value.code == errcode.ErrUnauthorized
    assert got.value.reason == BA.AUTH_REJECT_NO_USABLE_CREDENTIAL
    await _assert_auth_untouched(rdb, match_id, snap)


async def test_heartbeat_rejects_a_credential_whose_jti_differs(rdb, repo) -> None:
    """★ 四元组第 4 位:`jti` 不符 → `no_usable_credential`,零副作用。

    同 uid/epoch/gen 但 jti 不同 = **同一代际重签的另一张票**。gen 三道门(水位、
    active 门、全等)全都放行它,只有 jti 这一位拦得住;而 `jti` 恰恰是 §9 不变量 3
    里 B1 纯本地验票下的唯一吊销手段。

    ★ 变异:把 `battle_credential_matches` 里 `and _str_eq(cred.jti, ident.jti)` 删掉
      → 本条红。
    """
    match_id = 9_104
    _, _, ident = await _activate(rdb, repo, match_id)
    snap = await _snapshot_auth(rdb, match_id)

    bad = dataclasses.replace(ident, jti=ident.jti + "-forged")
    with pytest.raises(BA.BattleAuthStaleError) as got:
        await repo.activate_heartbeat(match_id, bad, _hb(state="running"))
    assert got.value.code == errcode.ErrUnauthorized
    assert got.value.reason == BA.AUTH_REJECT_NO_USABLE_CREDENTIAL
    await _assert_auth_untouched(rdb, match_id, snap)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("exp_ms", FAR_EXP_MS - 1),
        ("kid", "kid-forged"),
        ("token_sha256", "sha-forged"),
    ],
)
async def test_heartbeat_rejects_a_credential_whose_integrity_binding_differs(
    rdb, repo, field: str, value: object
) -> None:
    """★ 四元组之外的**完整性绑定**同样逐位比:`exp_ms` / `kid` / `token_sha256`。

    它们不是"锦上添花":`token_sha256` 把 Redis 记录钉到**具体那一串 token 字节**上,
    `kid` 钉到具体签名密钥,`exp_ms` 钉到具体有效期。任一位退化成不比,持票人就能在
    同一四元组下换一张自签 / 换密钥 / 改到期的票继续被认。

    ★ 变异:把 `battle_credential_matches` 里对应那一行(`cred.exp_ms == ident.exp_ms`
      / `_str_eq(cred.kid, ident.kid)` / `_str_eq(cred.token_sha256, ident.token_sha256)`)
      删掉 → 对应那条参数红。
    """
    match_id = 9_105 + hash(field) % 7  # 同族用例互不撞 match
    _, _, ident = await _activate(rdb, repo, match_id)
    snap = await _snapshot_auth(rdb, match_id)

    bad = dataclasses.replace(ident, **{field: value})
    with pytest.raises(BA.BattleAuthStaleError) as got:
        await repo.activate_heartbeat(match_id, bad, _hb(state="running"))
    assert got.value.code == errcode.ErrUnauthorized
    assert got.value.reason == BA.AUTH_REJECT_NO_USABLE_CREDENTIAL
    await _assert_auth_untouched(rdb, match_id, snap)


# ══ ② gen 水位:烧掉的代际永不复用 ══════════════════════════════════════════


async def test_stage_pending_refuses_a_gen_already_burned_by_high_water(rdb, repo) -> None:
    """★ **gen 复用被 `high_water_gen` 挡住** —— 同一个号只许 Stage 一次。

    构造刻意让 `authgen` 计数器门放行(counter 恰等于待 stage 的 gen),只剩水位门:
    先正常 Stage 一份 gen=N,再拿**同 gen、不同 jti** 的另一张票来 Stage。此时
      - 计数器门:counter == N == cred.gen ✓ 放行
      - 幂等门:pending 与 active 都不等于它(jti 不同)✓ 不短路
      - 水位门:N <= high_water(=N) → 必须拒
    少了水位门,一张"同代际重签的票"就能顶掉当前 pending —— 等于凭据吊销从未生效。

    ★ 变异:把 `stage_pending` 里 `if inp.credential.gen <= auth.high_water_gen or (...)`
      的 `<=` 改成 `<` → 本条红。
    """
    match_id = 9_110
    allocation_id = _alloc_id()
    await _seed_warming_battle(rdb, match_id, allocation_id)
    seed = await repo.prepare_credential(_binding(match_id, allocation_id))
    first = _cred(seed, jti="jti-first")
    rec = await repo.stage_pending(
        BA.BattleStageInput(
            match_id=match_id,
            allocation_id=allocation_id,
            credential=first,
            auth_ttl_sec=AUTH_TTL,
        )
    )
    assert rec.pending.jti == "jti-first"
    assert rec.high_water_gen == seed.gen

    snap = await _snapshot_auth(rdb, match_id)
    reused = _cred(seed, jti="jti-reused")
    with pytest.raises(BA.BattleAuthStaleError) as got:
        await repo.stage_pending(
            BA.BattleStageInput(
                match_id=match_id,
                allocation_id=allocation_id,
                credential=reused,
                auth_ttl_sec=AUTH_TTL,
            )
        )
    assert got.value.code == errcode.ErrUnauthorized
    await _assert_auth_untouched(rdb, match_id, snap)
    assert (await _auth(rdb, match_id)).pending.jti == "jti-first", "被拒的重签污染了 pending"


async def test_stage_pending_refuses_a_gen_that_is_not_the_latest_drawn_number(rdb, repo) -> None:
    """★ 第二道门:待 Stage 的 gen 必须**正好等于** `authgen` 计数器当前值。

    auth 键被清理后重建时 `high_water_gen=0`,只靠水位门会让一张旧票重新 stage 进来。
    计数器键刻意**永不过期**,于是"最新领取号"是唯一跨 auth 重建仍然记得的事实。
    构造:连续 Prepare 两次(领到 N、N+1),再拿 N 去 Stage —— 计数器已是 N+1,必须拒。

    ★ 变异:把 `stage_pending` 里 `if counter != inp.credential.gen: raise` 改成
      `if counter < inp.credential.gen:` → 本条红。
    """
    match_id = 9_111
    allocation_id = _alloc_id()
    await _seed_warming_battle(rdb, match_id, allocation_id)
    stale_seed = await repo.prepare_credential(_binding(match_id, allocation_id))
    fresh_seed = await repo.prepare_credential(_binding(match_id, allocation_id))
    assert fresh_seed.gen == stale_seed.gen + 1, "Prepare 必须严格递增取号"

    with pytest.raises(BA.BattleAuthStaleError):
        await repo.stage_pending(
            BA.BattleStageInput(
                match_id=match_id,
                allocation_id=allocation_id,
                credential=_cred(stale_seed),
                auth_ttl_sec=AUTH_TTL,
            )
        )
    assert not (await _auth(rdb, match_id)).HasField("pending")


async def test_heartbeat_refuses_a_gen_above_the_authority_high_water(rdb, repo) -> None:
    """★ 反向水位门:**上报代际高于权威水位 = 这张票不可能出自本权威**。

    构造上把记录里的 `high_water_gen` 压到 `active.gen - 1`(模拟"权威侧水位比 DS
    自称的代际低"),其余一切照旧。此时凭据全等比对仍会通过(active 就是它),
    只有这道门拦得住 —— 它挡的是伪造 / 别处签发的高代际票。

    ★ 变异:把 `activate_heartbeat` 里 `if auth.high_water_gen < ident.gen:` 整段删掉
      → 本条红(心跳被接受)。
    """
    match_id = 9_112
    _, _, ident = await _activate(rdb, repo, match_id)

    rec = await _auth(rdb, match_id)
    rec.high_water_gen = rec.active.gen - 1
    await rdb.set(BA.battle_auth_key(match_id), rec.SerializeToString(), px=int(AUTH_TTL * 1000))
    snap = await _snapshot_auth(rdb, match_id)

    with pytest.raises(BA.BattleAuthStaleError) as got:
        await repo.activate_heartbeat(match_id, ident, _hb(state="running"))
    assert got.value.reason == BA.AUTH_REJECT_GEN_BELOW_HIGH_WATER
    await _assert_auth_untouched(rdb, match_id, snap)


# ══ ③ 旧 epoch 的迟到写被 fencing ═════════════════════════════════════════════


async def test_a_late_write_from_the_previous_instance_epoch_is_fenced(rdb, repo) -> None:
    """★ **旧 epoch 的迟到写被 fencing 拒绝**,且新实例的授权记录一个字节都不许被污染。

    时序就是生产里最常见的那一幕:
      1. 实例 A(epoch=1)正常激活并在心跳;
      2. A 所在 Pod 被判死 / 重分配,新实例 B 走 Prepare → epoch=2、清 active/pending;
      3. A 的心跳(网络重试 / 进程还没退)迟到抵达。
    第 3 步必须被 `authority_binding_mismatch` 拒掉。放它进来的话,A 会把 battle 投影
    改回自己的 gen/jti,于是 B 明明是唯一 owner,权威记录却指向 A —— §9 不变量 22 的
    "同一时刻只有一个可玩 DS"从这里塌。

    ★ 变异:把 `prepare_credential` 换实例分支里的 `auth.instance_epoch += 1` 删掉
      → 本条红(epoch 不推进,旧票继续有效)。
    """
    match_id = 9_120
    allocation_id, _, old_ident = await _activate(rdb, repo, match_id)
    assert old_ident.instance_epoch == 1

    # 换实例:同 allocation/pod,但 GameServer UID 换代。
    battle = await _battle(rdb, match_id)
    battle.gameserver_uid = ""  # 编排层换实例时会先清掉旧 UID 绑定
    battle.state = "warming"
    await rdb.set(BA.battle_key(match_id), battle.SerializeToString())
    seed_b = await repo.prepare_credential(_binding(match_id, allocation_id, uid=UID_B))
    assert seed_b.instance_epoch == 2, "换实例必须推进 instance_epoch"

    rec = await _auth(rdb, match_id)
    assert not rec.HasField("active"), "换实例必须清掉旧 active"
    assert rec.high_water_gen >= old_ident.gen, "gen 水位必须单调保留"
    snap = await _snapshot_auth(rdb, match_id)

    with pytest.raises(BA.BattleAuthStaleError) as got:
        await repo.activate_heartbeat(match_id, old_ident, _hb(state="running"))
    assert got.value.reason == BA.AUTH_REJECT_BINDING_MISMATCH
    await _assert_auth_untouched(rdb, match_id, snap)


async def test_check_active_refuses_the_previous_epoch_credential(rdb, repo) -> None:
    """★ 受保护副作用 RPC 的前置门 `check_active` 同样按 exact 实例拒旧 epoch。

    `check_active` 是所有 Battle DS 副作用 RPC 的唯一入口门;它若只看"有没有 active",
    旧实例就能在新实例接管后继续发 GM / 结果写。断言用**具体错误码**而不是"抛了异常"。

    ★ 变异:把 `check_active` 里 `or not battle_credential_matches(...)` 删掉 → 本条红。
    """
    match_id = 9_121
    _, _, ident = await _activate(rdb, repo, match_id)
    await repo.check_active(match_id, ident)  # 正样本:当前凭据必须通过

    stale = dataclasses.replace(ident, instance_epoch=ident.instance_epoch + 1)
    with pytest.raises(BA.BattleAuthStaleError) as got:
        await repo.check_active(match_id, stale)
    assert got.value.code == errcode.ErrUnauthorized


# ══ ④ QUARANTINED:实例级永久墓碑 ═════════════════════════════════════════════


async def test_quarantine_revokes_the_credential_and_abandons_the_projection(rdb, repo) -> None:
    """★ 隔离(泄露 token 紧急路径)必须**同一事务**吊销授权 + 判弃投影 + 永久保留墓碑。

    三件事缺一不可:
      - `phase=QUARANTINED`:泄露 token 立刻失效;
      - `battle.state=abandoned`:进入可靠补偿 outbox,资源会被回收;
      - **两键都 PERSIST**(TTL 清零):墓碑过期后,另一个 UID 就能靠 Prepare 重建授权,
        隔离等于自动到期解除。

    还断言 `ACTIVE_KEY` 的 score 被写成 `0` —— 那是"下一轮 sweep 立即对账"的哨兵。

    ★ 变异:把 `quarantine_expected` 的 `_set(pipe, a_key, auth_payload, 0.0)` 改成
      `auth_ttl_sec` → 本条红(auth 键仍带 TTL)。
    """
    match_id = 9_130
    allocation_id, _, ident = await _activate(rdb, repo, match_id)

    result = await repo.quarantine_expected(
        match_id,
        BA.BattleQuarantineExpected(allocation_id=allocation_id, credential=ident),
        AUTH_TTL,
        BATTLE_TTL,
    )
    assert result.auth_quarantined is True
    assert result.projection_abandoned is True

    rec = await _auth(rdb, match_id)
    assert rec.phase == dspb.BATTLE_AUTH_PHASE_QUARANTINED, enum_name(
        dspb.BattleAuthPhase, rec.phase
    )
    assert not rec.HasField("pending"), "隔离必须清 pending"
    assert rec.delivered_rv == ""
    assert (await _battle(rdb, match_id)).state == "abandoned"
    assert await rdb.pttl(BA.battle_auth_key(match_id)) == -1, "墓碑不得带 TTL"
    assert await rdb.pttl(BA.battle_key(match_id)) == -1, "被判弃的投影不得带 TTL"
    assert await rdb.zscore(BA.ACTIVE_KEY, str(match_id)) == 0.0


async def test_quarantine_ignores_a_stale_expected_credential(rdb, repo) -> None:
    """★ 隔离必须带 exact 凭据:`expected` 对不上时**零变更**返回,不隔离。

    这条防的是"旧运维请求误隔离同名重建实例":一条几分钟前发出的隔离指令,不该把
    此刻正在服役的新实例打死。断言返回值两个位都是 False,并且 phase / battle 均未变。

    ★ 变异:删掉 `quarantine_expected` 里
      `or not battle_credential_matches(_active_of(auth_record), expected.credential, ...)`
      → 本条红。
    """
    match_id = 9_131
    allocation_id, _, ident = await _activate(rdb, repo, match_id)
    snap = await _snapshot_auth(rdb, match_id)

    wrong = dataclasses.replace(ident, jti=ident.jti + "-old")
    result = await repo.quarantine_expected(
        match_id,
        BA.BattleQuarantineExpected(allocation_id=allocation_id, credential=wrong),
        AUTH_TTL,
        BATTLE_TTL,
    )
    assert result.auth_quarantined is False
    assert result.projection_abandoned is False
    await _assert_auth_untouched(rdb, match_id, snap)
    assert (await _auth(rdb, match_id)).phase == dspb.BATTLE_AUTH_PHASE_ACTIVE
    assert (await _battle(rdb, match_id)).state == "ready"


async def test_heartbeat_from_a_quarantined_instance_is_locked_out(rdb, repo) -> None:
    """★ 隔离后,**原本完全合法**的那张凭据也必须被 `auth_phase_locked` 拒死。

    这是"隔离"与"轮换"的分水岭:轮换只是换票,隔离是判这台实例出局。凭据本身依旧
    未过期、四元组依旧全对 —— 唯一改变的是 phase。相位判据一旦漏,泄露 token 在隔离
    之后照样能写。

    ★ 变异:把 `battle_auth_phase_locked` 里的 `BATTLE_AUTH_PHASE_QUARANTINED` 去掉
      → 本条红。
    """
    match_id = 9_132
    allocation_id, _, ident = await _activate(rdb, repo, match_id)
    await repo.quarantine_expected(
        match_id,
        BA.BattleQuarantineExpected(allocation_id=allocation_id, credential=ident),
        AUTH_TTL,
        BATTLE_TTL,
    )
    snap = await _snapshot_auth(rdb, match_id, pin_pttl_ms=5_000)

    with pytest.raises(BA.BattleAuthStaleError) as got:
        await repo.activate_heartbeat(match_id, ident, _hb(state="running"))
    assert got.value.reason == BA.AUTH_REJECT_PHASE_LOCKED
    await _assert_auth_untouched(rdb, match_id, snap)


async def test_quarantine_tombstone_blocks_prepare_even_from_a_different_instance(
    rdb, repo
) -> None:
    """★ 墓碑必须**先于** same-instance 判断生效 —— 换个 UID 也不许重开授权。

    产品代码里那条注释("必须先于 same_instance 判断拒绝")指的就是本条:若把相位
    检查放到"换实例"分支之后,伪造一个不同 UID 就会走进重置分支,把 tombstone
    连同 active/pending 一起清空,隔离被一次 Prepare 撤销。

    ★ 变异:把 `prepare_credential` 里的 `if battle_auth_phase_locked(auth.phase): raise`
      移到 `same_instance` 分支内部(或整段删掉)→ 本条红。
    """
    match_id = 9_133
    allocation_id, _, ident = await _activate(rdb, repo, match_id)
    await repo.quarantine_expected(
        match_id,
        BA.BattleQuarantineExpected(allocation_id=allocation_id, credential=ident),
        AUTH_TTL,
        BATTLE_TTL,
    )
    # battle 已 abandoned;把它摆回 warming 以排除"状态门先拦住"的干扰,
    # 让本条只测相位墓碑那一格。
    battle = await _battle(rdb, match_id)
    battle.state = "warming"
    battle.gameserver_uid = ""
    await rdb.set(BA.battle_key(match_id), battle.SerializeToString())

    with pytest.raises(BA.BattleAuthStaleError):
        await repo.prepare_credential(_binding(match_id, allocation_id, uid=UID_B))
    rec = await _auth(rdb, match_id)
    assert rec.phase == dspb.BATTLE_AUTH_PHASE_QUARANTINED, "墓碑被 Prepare 清掉了"
    assert rec.instance_uid == UID_A, "墓碑被改绑到了新实例"


# ══ ⑤ ROTATING:新旧凭据并存,互不串味 ═════════════════════════════════════════


async def test_rotation_keeps_the_old_credential_serving_until_the_new_one_is_delivered(
    rdb, repo
) -> None:
    """★ 轮换期间 **active(旧)与 pending(新)并存**,且各自只认自己那一张票。

    四段断言对应轮换的四个不变量:
      1. Stage 新票后 phase=ROTATING,active 仍是旧票 —— 旧 DS 不能因为签了新票就掉线;
      2. 旧票心跳继续被接受(promote=False)—— 轮换不是中断;
      3. 新票在 `delivered_rv` 落地前**不可用**(`no_usable_credential`)—— 凭据必须
         先真正投递到 GameServer annotation,否则会出现"权威认新票、DS 手里还是旧票";
      4. MarkDelivered 后新票一次提升成 ACTIVE,pending 被清空。

    ★ 变异:把 `activate_heartbeat` 提升分支的 `and auth.delivered_rv != ""` 删掉
      → 第 3 段红(未投递的新票被直接提升)。
    """
    match_id = 9_140
    allocation_id, old_cred, old_ident = await _activate(rdb, repo, match_id)

    # ── 1. Prepare + Stage 新一代 ──
    seed2 = await repo.prepare_credential(_binding(match_id, allocation_id))
    assert seed2.instance_epoch == 1, "同实例轮换不得推进 instance_epoch"
    assert seed2.gen == old_cred.gen + 1
    new_cred = _cred(seed2, jti="jti-rotated")
    rec = await repo.stage_pending(
        BA.BattleStageInput(
            match_id=match_id,
            allocation_id=allocation_id,
            credential=new_cred,
            auth_ttl_sec=AUTH_TTL,
        )
    )
    assert rec.phase == dspb.BATTLE_AUTH_PHASE_ROTATING, enum_name(
        dspb.BattleAuthPhase, rec.phase
    )
    assert rec.active.jti == old_cred.jti, "轮换不得提前顶掉 active"
    assert rec.pending.jti == "jti-rotated"
    assert rec.high_water_gen == new_cred.gen

    # ── 2. 旧票仍在服役 ──
    out_old = await repo.activate_heartbeat(match_id, old_ident, _hb(state="running"))
    assert out_old.first_activation is False
    assert out_old.active.jti == old_cred.jti

    # ── 3. 新票未投递 → 不可用 ──
    new_ident = _ident(new_cred)
    with pytest.raises(BA.BattleAuthStaleError) as got:
        await repo.activate_heartbeat(match_id, new_ident, _hb(state="running"))
    assert got.value.reason == BA.AUTH_REJECT_NO_USABLE_CREDENTIAL
    assert (await _auth(rdb, match_id)).active.jti == old_cred.jti

    # ── 4. 投递后提升 ──
    await repo.mark_delivered(match_id, allocation_id, new_cred, "rv-2", AUTH_TTL)
    out_new = await repo.activate_heartbeat(match_id, new_ident, _hb(state="running"))
    assert out_new.first_activation is True
    assert out_new.active.jti == "jti-rotated"
    rec = await _auth(rdb, match_id)
    assert rec.phase == dspb.BATTLE_AUTH_PHASE_ACTIVE
    assert not rec.HasField("pending"), "提升后必须清 pending"
    assert rec.delivered_rv == ""


async def test_after_rotation_the_previous_credential_stops_being_authoritative(
    rdb, repo
) -> None:
    """★ 轮换完成后旧票立刻失效 —— "并存"只在投递窗口内成立,不是永久双票。

    若旧票在提升后仍被接受,那就是同一实例上两张同时有效的票:吊销新票不再等于
    吊销写权限,§9 不变量 3 的 jti 吊销手段被架空。

    ★ 变异:把 `activate_heartbeat` 提升分支里的 `auth.ClearField("pending")` 换成
      不清、并让旧 active 保留在某处 → 本条红(此处直接断言旧票被拒)。
    """
    match_id = 9_141
    allocation_id, old_cred, old_ident = await _activate(rdb, repo, match_id)
    seed2 = await repo.prepare_credential(_binding(match_id, allocation_id))
    new_cred = _cred(seed2, jti="jti-rotated")
    await repo.stage_pending(
        BA.BattleStageInput(
            match_id=match_id,
            allocation_id=allocation_id,
            credential=new_cred,
            auth_ttl_sec=AUTH_TTL,
        )
    )
    await repo.mark_delivered(match_id, allocation_id, new_cred, "rv-2", AUTH_TTL)
    await repo.activate_heartbeat(match_id, _ident(new_cred), _hb(state="running"))

    snap = await _snapshot_auth(rdb, match_id)
    with pytest.raises(BA.BattleAuthStaleError) as got:
        await repo.activate_heartbeat(match_id, old_ident, _hb(state="running"))
    assert got.value.reason == BA.AUTH_REJECT_NO_USABLE_CREDENTIAL
    await _assert_auth_untouched(rdb, match_id, snap)
    assert (await _auth(rdb, match_id)).active.jti != old_cred.jti


async def test_mark_delivered_only_accepts_the_current_pending_credential(rdb, repo) -> None:
    """★ `MarkDelivered` 只认当前 pending;旧 PATCH 的晚响应必须**零变更**。

    投递是异步的:一次针对**上一代** pending 的 PATCH 可能在新一代 Stage 之后才回来。
    若它能写 `delivered_rv`,新一代 pending 就会被"上一代的投递证明"错误地放行提升。

    ★ 变异:删掉 `mark_delivered` 里
      `or not battle_credential_equal(_pending_of(auth), expected)` → 本条红。
    """
    match_id = 9_142
    allocation_id, _, _ = await _activate(rdb, repo, match_id)
    seed2 = await repo.prepare_credential(_binding(match_id, allocation_id))
    new_cred = _cred(seed2, jti="jti-rotated")
    await repo.stage_pending(
        BA.BattleStageInput(
            match_id=match_id,
            allocation_id=allocation_id,
            credential=new_cred,
            auth_ttl_sec=AUTH_TTL,
        )
    )
    snap = await _snapshot_auth(rdb, match_id)

    ghost = _cred(seed2, jti="jti-previous-patch")
    with pytest.raises(BA.BattleAuthStaleError) as got:
        await repo.mark_delivered(match_id, allocation_id, ghost, "rv-ghost", AUTH_TTL)
    assert got.value.code == errcode.ErrUnauthorized
    await _assert_auth_untouched(rdb, match_id, snap)
    assert (await _auth(rdb, match_id)).delivered_rv == ""


# ══ ⑥ 两阶段激活稳定性门(首次激活专用)═══════════════════════════════════════


async def test_first_activation_requires_the_stability_evidence_and_changes_nothing_before_it(
    rdb, repo
) -> None:
    """★ 稳定性门未满足时**零状态转移**:不提升、不写 auth/battle、battle 保持 warming。

    首拍只能证明"此刻活着"。一台起来就崩的 DS 会在崩溃前发出恰好一拍心跳,如果那一拍
    就能提升 ACTIVE,匹配侧会把玩家送进一台马上消失的 DS。

    还顺带钉住"门开关判据是 **and** 不是 or":本用例配 `beats=2, span_ms=0`,
    写成 `or` 会让 `span_ms<=0` 直接把整道门关掉,首拍即激活。

    ★ 变异:把 `battle_activation_stability_pending` 的
      `if inp.stability_beats <= 1 and inp.stability_span_ms <= 0:` 改成 `or` → 本条红。
    """
    match_id = 9_150
    allocation_id = _alloc_id()
    await _seed_warming_battle(rdb, match_id, allocation_id)
    seed = await repo.prepare_credential(_binding(match_id, allocation_id))
    cred = _cred(seed)
    await repo.stage_pending(
        BA.BattleStageInput(
            match_id=match_id,
            allocation_id=allocation_id,
            credential=cred,
            auth_ttl_sec=AUTH_TTL,
        )
    )
    await repo.mark_delivered(match_id, allocation_id, cred, "rv-1", AUTH_TTL)
    ident = _ident(cred)

    first = await repo.activate_heartbeat(match_id, ident, _hb(state="ready", beats=2))
    assert first.activation_pending is True
    assert first.first_activation is False
    assert first.battle.state == "warming", "证据不足时 battle 必须保持 warming"
    rec = await _auth(rdb, match_id)
    assert not rec.HasField("active"), "证据不足时不得提升 ACTIVE"
    assert rec.phase == dspb.BATTLE_AUTH_PHASE_BOOTSTRAP
    assert rec.last_active_heartbeat_ms == 0, "证据不足时不得写心跳时刻"

    second = await repo.activate_heartbeat(match_id, ident, _hb(state="ready", beats=2))
    assert second.activation_pending is False
    assert second.first_activation is True
    assert (await _battle(rdb, match_id)).state == "ready"


# ══ ⑦ fail-closed:读不到 ≠ 读不了 ═══════════════════════════════════════════


async def test_heartbeat_fails_closed_when_the_battle_projection_key_is_missing(
    rdb, repo
) -> None:
    """★ battle 键缺失 → `BattleAuthStaleError`(拒),**绝不**当成"没这局所以放行"。

    `_read_bound_authority` 的契约:任一键缺失 = 权威不可读 = fail-closed。
    把缺键读成"允许"的话,一次 Redis 逐出就会让任何持票 DS 在任何 match 上写入。

    ★ 变异:把 `_read_bound_authority` 里 `if auth is None or battle is None: raise`
      改成只判 `auth is None` → 本条红。
    """
    match_id = 9_160
    _, _, ident = await _activate(rdb, repo, match_id)
    await rdb.delete(BA.battle_key(match_id))
    snap = await _snapshot_auth(rdb, match_id)

    with pytest.raises(BA.BattleAuthStaleError) as got:
        await repo.activate_heartbeat(match_id, ident, _hb(state="running"))
    assert got.value.code == errcode.ErrUnauthorized
    await _assert_auth_untouched(rdb, match_id, snap)


async def test_heartbeat_fails_closed_when_the_auth_key_is_missing(rdb, repo) -> None:
    """★ auth 键缺失同样 fail-closed,且**不许顺手重建**一条授权记录。

    "记录没了就当首次,建一条新的"是最诱人的错误修法:它让 DS 立刻恢复心跳,
    代价是任何人只要能删掉 auth 键就能重新获得授权。

    ★ 变异:在 `_read_bound_authority` 的 `auth is None` 分支里改成"建空记录继续"
      → 本条红。
    """
    match_id = 9_161
    _, _, ident = await _activate(rdb, repo, match_id)
    await rdb.delete(BA.battle_auth_key(match_id))

    with pytest.raises(BA.BattleAuthStaleError) as got:
        await repo.activate_heartbeat(match_id, ident, _hb(state="running"))
    assert got.value.code == errcode.ErrUnauthorized
    assert await rdb.get(BA.battle_auth_key(match_id)) is None, "拒绝路径重建了授权记录"


async def test_authority_pair_read_surfaces_wrongtype_instead_of_pretending_absence(
    rdb, repo
) -> None:
    """★ **"读不了"不得伪装成"读不到"**:auth 键类型错误必须原样冒泡 Redis 错误。

    这正是产品代码用单条 Lua(`GET`+`GET`)而不是 `MGET` 的理由:`MGET` 对**存在但
    类型错误**的键静默返回 nil,于是 WRONGTYPE 被伪装成"合法缺失",下游据此推进
    abandoned / release 这类不可逆动作。Lua 里的 `redis.call('GET')` 直接抛。

    断言的是**错误类型**而不是"抛了异常":`BattleAuthStaleError` 也是异常,但它表示
    "查到了、判定为无权",与"根本没查成"是两件事,混淆就是 §9.22 的 UNKNOWN 冒充。

    ★ 变异:把 `_BATTLE_AUTHORITY_PAIR_SCRIPT` 换成 `MGET` 语义(或把 `_read_authority_pair_atomic`
      改成两次 `pipe.get`)→ 本条红(拿到的是 BattleAuthStaleError 而不是 ResponseError)。
    """
    match_id = 9_162
    _, _, ident = await _activate(rdb, repo, match_id)
    await rdb.delete(BA.battle_auth_key(match_id))
    await rdb.rpush(BA.battle_auth_key(match_id), b"not-a-string")

    with pytest.raises(redis_exc.ResponseError) as got:
        await repo.activate_heartbeat(match_id, ident, _hb(state="running"))
    assert "WRONGTYPE" in str(got.value).upper()


class _DeadRedis:
    """一个"每次访问都超时"的 Redis 替身,用来模拟 apiserver / Redis 不可达。

    只实现被测路径会碰到的入口;其余属性一律抛,避免"漏实现某个方法但测试仍绿"。
    """

    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    def pipeline(self, *args: object, **kwargs: object) -> object:
        raise self._exc

    async def zadd(self, *args: object, **kwargs: object) -> object:
        raise self._exc

    async def zrem(self, *args: object, **kwargs: object) -> object:
        raise self._exc


async def test_check_active_propagates_a_redis_failure_instead_of_returning_ok() -> None:
    """★ **查询失败必须冒泡,绝不能冒充"校验通过"**。

    `check_active` 成功时返回 `None`。这里让 Redis 每次都超时:正确行为是把
    `TimeoutError` 抛出去(调用方 fail-closed 拒绝这次 DS 副作用 RPC);
    错误行为是吞掉异常 `return`,那样任何一次 Redis 抖动都会让**所有** DS 写无条件放行。

    ★ 变异:给 `read_authority` 的 CAS 循环加一个
      `except Exception: return BattleAuthoritySnapshot()` → 本条红
      (变成 BattleAuthStaleError,而不是超时冒泡)。
    """
    boom = redis_exc.TimeoutError("redis timed out")
    repo = BA.RedisBattleAuthRepo(_DeadRedis(boom))
    ident = BA.BattleCredentialIdentity(
        pod_name=POD,
        instance_uid=UID_A,
        instance_epoch=1,
        gen=1,
        jti="jti-1",
        exp_ms=FAR_EXP_MS,
        kid=KID,
        token_sha256="sha-1",
        writer_epoch=WRITER,
    )
    with pytest.raises(redis_exc.TimeoutError):
        await repo.check_active(9_163, ident)


async def test_read_authority_never_reports_found_for_an_unreadable_key(rdb) -> None:
    """★ `read_authority` 的 `*_found` 位是**证据**,不是默认值。

    快照两个 found 位缺省必须是 False,并且只有真的读到字节才置 True。若把"读失败"
    也算 found,`ready_authorized` / `heartbeat_fresh` 都会在一条空记录上给出肯定答案。

    ★ 变异:把 `read_authority` 里 `out.auth_found = True` 提到 `if a_raw is not None:`
      之外 → 本条红。
    """
    repo = BA.RedisBattleAuthRepo(rdb)
    snapshot = await repo.read_authority(9_164)
    assert snapshot.auth_found is False
    assert snapshot.battle_found is False
    ok, reason = snapshot.ready_authorized(BA.now_ms(), 30_000)
    assert ok is False
    assert reason == "auth-missing"


async def test_pop_commands_leaves_the_queue_untouched_when_authority_is_missing(
    rdb, repo
) -> None:
    """★ 权威校验不过时,命令队列必须**逐字节不变**(连 MULTI/EXEC 都不该发出)。

    队列是"发给这台 DS 的指令"。校验失败仍 RPOP 的话,指令会被一台无权的 DS 吃掉并
    永久丢失 —— 真正的 owner 再也收不到 stop / drain,表现为"驱逐指令发了但没人执行"。

    ★ 变异:把 `pop_commands_if_active` 里
      `if not ok or battle_terminal(battle.state): raise` 删掉 → 本条红。
    """
    match_id = 9_165
    _, _, ident = await _activate(rdb, repo, match_id)
    queue_key = f"pandora:ds:cmd:{{{match_id}}}"
    await rdb.rpush(queue_key, b"cmd-1", b"cmd-2")

    # 正样本:合法凭据能取到指令(RPOP 从右侧取,先出 cmd-2)。
    got = await repo.pop_commands_if_active(match_id, ident, queue_key, 1)
    assert got == [b"cmd-2"]

    # 负样本:auth 被删 → 拒绝 + 队列不动。
    await rdb.delete(BA.battle_auth_key(match_id))
    with pytest.raises(BA.BattleAuthStaleError):
        await repo.pop_commands_if_active(match_id, ident, queue_key, 1)
    assert await rdb.lrange(queue_key, 0, -1) == [b"cmd-1"]


async def test_pop_commands_refuses_a_queue_outside_the_match_hash_tag(rdb, repo) -> None:
    """★ 队列键必须与 auth 键**同 slot**,否则整个"权威校验与 RPOP 同事务"不成立。

    Redis Cluster 上跨 slot 的 MULTI 会被整条拒;但真正危险的是单机 Redis 上它"正常
    工作",于是这条约束直到上了 Cluster 才爆。把它做成入参校验,单机也拦得住。

    ★ 变异:把 `pop_commands_if_active` 里 `or not contains_battle_hash_tag(queue_key, match_id)`
      删掉 → 本条红。
    """
    match_id = 9_166
    _, _, ident = await _activate(rdb, repo, match_id)
    with pytest.raises(errcode.PandoraError) as got:
        await repo.pop_commands_if_active(match_id, ident, "pandora:ds:cmd:{999}", 1)
    assert got.value.code == errcode.ErrInvalidArg


async def test_prepare_credential_fails_closed_without_a_battle_projection(rdb, repo) -> None:
    """★ 没有 battle 镜像就不许签发凭据 —— Prepare 不是"顺手建一局"。

    `_read_battle_from` 的注释写得很明确:缺失 = 权威不可读,不是"没分配"。
    若这里退化成"没有就建",任何人都能凭一个 match_id 让 allocator 发出一张真票。

    ★ 变异:把 `_read_battle_from` 里 `if raw is None: raise BattleAuthStaleError`
      改成返回一条空记录 → 本条红。
    """
    match_id = 9_167
    with pytest.raises(BA.BattleAuthStaleError):
        await repo.prepare_credential(_binding(match_id, _alloc_id()))
    assert await rdb.get(BA.battle_auth_key(match_id)) is None
    assert await rdb.get(BA.battle_auth_gen_key(match_id)) is None, "被拒的 Prepare 领了号"


async def test_prepare_credential_refuses_a_binding_that_is_not_writer_epoch_v2(rdb, repo) -> None:
    """★ `required_writer_epoch` 必须**恰好等于** V2,不接受更低也不接受更高。

    更低 = 旧 writer 写不出满足 Model B 的记录;更高 = 未来 writer 的记录只能由对应
    未来二进制处理。本版本一律拒,**不做隐式迁移** —— 隐式迁移就是滚动升级期两个
    版本互相改写对方不理解的字段。

    ★ 变异:把 `_validate_battle_binding` 里
      `or binding.required_writer_epoch != BATTLE_DS_WRITER_EPOCH_V2` 改成 `<` → 本条红。
    """
    match_id = 9_168
    allocation_id = _alloc_id()
    await _seed_warming_battle(rdb, match_id, allocation_id)
    binding = dataclasses.replace(
        _binding(match_id, allocation_id), required_writer_epoch=WRITER + 1
    )
    with pytest.raises(errcode.PandoraError) as got:
        await repo.prepare_credential(binding)
    assert got.value.code == errcode.ErrInvalidArg
    assert await rdb.get(BA.battle_auth_key(match_id)) is None


async def test_gen_counter_never_expires_so_a_burned_number_cannot_come_back(rdb, repo) -> None:
    """★ `authgen` 计数器**永不设 TTL**:auth 键过期也不能让 gen 回退。

    这是 ② 那一族门的物理基础。计数器一旦跟着 auth 键一起过期,重建后的第 1 代
    与被吊销的第 1 代同号,`stage_pending` 的计数器门就会把旧票放行。

    ★ 变异:把 `prepare_credential` 里 `_set(pipe, g_key, counter, 0.0)` 的最后一个
      参数改成 `binding.auth_ttl_sec` → 本条红。
    """
    match_id = 9_169
    allocation_id = _alloc_id()
    await _seed_warming_battle(rdb, match_id, allocation_id)
    seed = await repo.prepare_credential(_binding(match_id, allocation_id))
    assert seed.gen == 1
    assert await rdb.pttl(BA.battle_auth_gen_key(match_id)) == -1, "代际计数器带上了 TTL"

    # auth 键被清理后重建,取号必须继续从 2 开始而不是回到 1。
    await rdb.delete(BA.battle_auth_key(match_id))
    again = await repo.prepare_credential(_binding(match_id, allocation_id))
    assert again.gen == 2, "auth 键重建后代际回退了"
