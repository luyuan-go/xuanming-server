"""Model B「Redis 唯一授权权威」授权记录状态机回归测试 ——
覆盖 `services/hub_allocator/auth_repo.py`。

对应 Go 侧 `internal/data/hub_auth_repo.go` + `hub_authoritative.go`,
断言清单取自 `hub_auth_repo_test.go` 与 `hub_authoritative_test.go`。

★ 与 `test_hub_allocator_ledger.py` 同一套 `rdb` 夹具:**优先真 Redis**
(默认 `127.0.0.1:16379`,docker 容器 `pandora-redis`),连不上才回落 `fakeredis`,
**绝不 skip**。理由同那份文件 —— 这里测的是「谁有权把一台 DS 变成可路由」的
状态机(§9.22 exact 实例绑定 + §9.6 五要件),给它留一条「环境不好就整体不跑」
的后门,等于把 Model B 最核心的几道门变成可选项:

    docker run -d --name pandora-redis -p 16379:6379 redis:8-alpine

★ 每个用例用**独立 pod 名**,避免跨用例污染。
★ 每条用例 docstring 里的 `★ 变异:` 都真跑过(改坏 → 红 → 改回 → 绿),不是照代码猜的。
"""

from __future__ import annotations

import asyncio
import os

import pytest
from pandora.hub.v1 import allocator_pb2 as hubpb

from pandorapy import errcode
from pandorapy.auth import DS_AUTH_WRITER_EPOCH_V2 as WRITER
from pandorapy.protoenum import enum_name
from pandorapy.services.hub_allocator import auth_repo as hauth
from pandorapy.services.hub_allocator import ledger as L

# ── fixture ─────────────────────────────────────────────────────────────────

#: 授权键 TTL 远长于分片键 TTL —— Go 侧 CE8 的同一组数量级(48h vs 30m)。
AUTH_TTL = 48 * 3600.0
SHARD_TTL = 1800.0

#: 所有 phase,用于「白名单之外一律拒」的全枚举遍历。手写子集会在 proto 新增
#: phase 时静默漏测,所以从 DESCRIPTOR 取全集。
ALL_PHASES = tuple(v.number for v in hubpb.HubAuthPhase.DESCRIPTOR.values)


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


# ── 构造辅助 ────────────────────────────────────────────────────────────────


def _cred(*, uid: str, epoch: int, gen: int, jti: str, kid: str = "kid-1"):
    """一份**完整且未过期**的存储凭据(Go: credFor)。

    exp_ms 用一个远期常量而不是 `now+1h`:凭据完整性判据里有 `exp_ms > at_ms`,
    用相对时间会让「跑得慢的一次 CI」把一条与到期无关的用例判红。
    """
    return hubpb.HubDSCredential(
        gen=gen,
        jti=jti,
        exp_ms=2_000_000_000_000,
        kid=kid,
        instance_uid=uid,
        protocol_epoch=epoch,
        token_sha256="sha-" + jti,
        writer_epoch=WRITER,
    )


def _ident(cred) -> hauth.CredentialIdentity:
    """把存储凭据翻成「DS 心跳携带的已验签身份」。"""
    return hauth.CredentialIdentity(
        gen=cred.gen,
        jti=cred.jti,
        instance_uid=cred.instance_uid,
        protocol_epoch=cred.protocol_epoch,
        token_sha256=cred.token_sha256,
        kid=cred.kid,
        writer_epoch=cred.writer_epoch,
    )


def _hb(*, capacity: int = 500, state: str = "", ts_ms: int = 0, players: int = 0):
    return hauth.ActivateHeartbeatInput(
        player_count=players,
        player_ids=tuple(range(1, players + 1)),
        max_players=capacity,
        state=state,
        ts_ms=ts_ms,
        auth_ttl_sec=AUTH_TTL,
        shard_ttl_sec=SHARD_TTL,
    )


async def _seed_warming_shard(rdb, pod: str, *, capacity: int = 500) -> None:
    """播种一个 warming 分片镜像(模拟拓扑种子,等首个鉴权心跳翻 ready)。"""
    shard = hubpb.HubShardStorageRecord(
        hub_pod_name=pod,
        hub_addr="10.0.0.1:7777",
        region="cn",
        shard_id=1,
        capacity=capacity,
        state="warming",
    )
    await rdb.set(L.shard_key(pod), L.marshal_shard(shard), px=int(SHARD_TTL * 1000))


async def _shard(rdb, pod: str) -> hubpb.HubShardStorageRecord:
    return L.unmarshal_shard(pod, await rdb.get(L.shard_key(pod)))


async def _activate_ready(rdb, pod: str, *, uid: str = "uid-A", gen: int = 7, capacity: int = 500):
    """把一台 Hub 引导到 ACTIVE + 分片 ready。返回 `(repo, cred, ident)`。

    刻意走真实链路(init → stage → activate),不手搓 Redis 字节:手搓少填一个
    投影字段,后面的路由用例就会因为一个与被测点无关的 reason 而绿得莫名其妙。
    """
    repo = hauth.RedisHubAuthRepo(rdb)
    await repo.init_auth(pod, uid, AUTH_TTL)
    cred = _cred(uid=uid, epoch=1, gen=gen, jti=f"j{gen}")
    await repo.stage_pending(pod, cred, AUTH_TTL)
    await _seed_warming_shard(rdb, pod, capacity=capacity)
    out = await repo.activate_heartbeat(
        pod, _ident(cred), _hb(capacity=capacity, state="ready")
    )
    assert out.accepted and out.shard_found, "引导失败:后续用例的前提不成立"
    return repo, cred, _ident(cred)


# ── ① InitAuth:建 / 幂等 / 换实例复位 ───────────────────────────────────────


async def test_init_auth_creates_bootstrap_bound_to_the_exact_instance(rdb) -> None:
    """★ 首建必须落 BOOTSTRAP + epoch=1 + required_writer_epoch=V2。

    `required_writer_epoch` 是 Model B 的机械激活栅栏。首建时不写死 V2,后面每一次
    `hub_auth_record_v2_exact` 都会判失败 —— 表现是这台 DS 永远激活不了,而日志里
    只有一句 AuthStale,看不出是「记录建歪了」还是「凭据不对」。

    ★ 变异:把 `init_auth` 里 `rec.required_writer_epoch = DS_AUTH_WRITER_EPOCH_V2`
      删掉(或改成 0)→ 本条红。
    """
    pod = "a01-init"
    repo = hauth.RedisHubAuthRepo(rdb)
    rec = await repo.init_auth(pod, "uid-A", AUTH_TTL)
    assert rec.phase == hubpb.HUB_AUTH_PHASE_BOOTSTRAP, enum_name(hubpb.HubAuthPhase, rec.phase)
    assert rec.instance_uid == "uid-A"
    assert rec.protocol_epoch == 1
    assert rec.required_writer_epoch == WRITER
    assert rec.pod_name == pod


async def test_init_auth_with_the_same_uid_never_bumps_the_epoch(rdb) -> None:
    """★ 同 uid 重复 Init 必须幂等 —— epoch 不许动。

    Fleet 对账每轮都会调 InitAuth。每轮 epoch++ 的话,DS 手里那张刚签好的凭据
    (绑着旧 epoch)下一秒就变 stale,于是**永远**追不上:分片始终 warming,
    玩家进不去大厅,而每一条日志都显示"正在正常投递令牌"。

    ★ 变异:把 `init_auth` 里 `if rec.instance_uid != instance_uid:` 改成无条件复位
      → 本条红。
    """
    pod = "a02-idem"
    repo = hauth.RedisHubAuthRepo(rdb)
    await repo.init_auth(pod, "uid-A", AUTH_TTL)
    again = await repo.init_auth(pod, "uid-A", AUTH_TTL)
    assert again.protocol_epoch == 1


async def test_init_auth_on_a_rebuilt_instance_resets_but_keeps_the_gen_high_water(rdb) -> None:
    """★ 换实例 → epoch++ 且清 active/pending,但 `high_water_gen` **必须保留**。

    两件事各治一种复活:
      - epoch++ 抗「代际计数器随 TTL 复位后重放旧 gen」;
      - high_water 保留保证 gen 水位单调不回退。
    把 high_water 一起清掉的话,复位后的第 1 代会被判成合法新代际 —— 一张早就
    该作废的旧令牌又能重新 stage 进来(§9.22 exact 实例绑定被打穿)。

    ★ 变异:在 `init_auth` 的复位分支里加一行 `rec.high_water_gen = 0` → 本条红。
    """
    pod = "a03-reset"
    repo = hauth.RedisHubAuthRepo(rdb)
    await repo.init_auth(pod, "uid-A", AUTH_TTL)
    await repo.stage_pending(pod, _cred(uid="uid-A", epoch=1, gen=5, jti="j5"), AUTH_TTL)

    rec = await repo.init_auth(pod, "uid-B", AUTH_TTL)
    assert rec.instance_uid == "uid-B"
    assert rec.protocol_epoch == 2
    assert not rec.HasField("pending"), "换实例必须清 pending"
    assert not rec.HasField("active"), "换实例必须清 active"
    assert rec.high_water_gen == 5, "gen 水位必须单调保留,否则旧代际可以复活"


async def test_init_auth_refuses_a_legacy_writer_record_without_touching_it(rdb) -> None:
    """★ 遇到非 V2 writer 的权威记录 → AuthStale,且**字节与 TTL 一个都不许动**。

    这条记录属于另一代 writer(未来二进制,或还没迁完的旧二进制)。当前副本
    "顺手修一下"就是在替一个自己不理解的协议做决定;而只要它刷了 TTL,
    这条坏记录就能永远续命下去。所以必须是零副作用的拒绝。

    ★ 变异:把 `init_auth` 里 `if rec.required_writer_epoch != DS_AUTH_WRITER_EPOCH_V2
      or not (...): raise AuthStaleError` 整段删掉 → 本条红。
    """
    pod = "a04-legacy"
    legacy = hubpb.HubShardAuthStorageRecord(
        pod_name=pod,
        instance_uid="uid-A",
        protocol_epoch=7,
        phase=hubpb.HUB_AUTH_PHASE_BOOTSTRAP,
        required_writer_epoch=0,
    )
    await rdb.set(L.auth_key(pod), legacy.SerializeToString(), px=int(AUTH_TTL * 1000))
    before = await rdb.get(L.auth_key(pod))
    before_ttl = await rdb.pttl(L.auth_key(pod))

    repo = hauth.RedisHubAuthRepo(rdb)
    with pytest.raises(L.AuthStaleError) as got:
        await repo.init_auth(pod, "uid-A", AUTH_TTL)
    assert got.value.code == errcode.ErrUnauthorized
    assert await rdb.get(L.auth_key(pod)) == before, "拒绝路径改写了记录字节"
    assert abs(await rdb.pttl(L.auth_key(pod)) - before_ttl) < 3_000, "拒绝路径刷了 TTL"


async def test_init_auth_requires_a_non_empty_instance_uid(rdb) -> None:
    """★ 空 uid 直接 ErrInvalidArg,不许建一条"没绑实例"的授权记录。

    空 uid 的记录会与**任意**空 uid 请求匹配上;而 §9.22 的整条 exact 实例绑定
    都建立在"uid 唯一标识这一次 GameServer 生命周期"之上。

    ★ 变异:删掉 `init_auth` 开头的 `if instance_uid == "": raise` → 本条红。
    """
    repo = hauth.RedisHubAuthRepo(rdb)
    with pytest.raises(errcode.PandoraError) as got:
        await repo.init_auth("a05-nouid", "", AUTH_TTL)
    assert got.value.code == errcode.ErrInvalidArg
    assert await rdb.get(L.auth_key("a05-nouid")) is None


# ── ② gen 单调门:已用过的代际不得重签 ───────────────────────────────────────


async def test_stage_pending_rejects_a_gen_the_high_water_already_burned(rdb) -> None:
    """★ **gen 复用被 `high_water_gen` 挡住** —— 用过的代际永远不得重签。

    gen 由 Redis INCR 发号,而计数器键是有 TTL 的:一次过期 / 一次误删,下一轮
    就会从头再发一遍同样的号。此时如果只比 `active.gen`,一张早已被吊销的旧令牌
    (gen 相同、jti 不同)就能重新 stage 进来并激活 —— 相当于凭据吊销从未生效。
    `high_water_gen` 是唯一记住"这个号已经烧掉了"的地方,且它跨复位保留。

    三段依次是:正常暂存 → 同 gen 重签 → 更低 gen 重签,后两者都必须 AuthStale。

    ★ 变异:把 `stage_pending` 里的 `if cred.gen <= rec.high_water_gen: raise`
      改成 `<`(或整条删掉)→ 本条红。
    """
    pod = "a06-gengate"
    repo = hauth.RedisHubAuthRepo(rdb)
    await repo.init_auth(pod, "uid-A", AUTH_TTL)

    rec = await repo.stage_pending(pod, _cred(uid="uid-A", epoch=1, gen=3, jti="j3"), AUTH_TTL)
    assert rec.pending.gen == 3
    assert rec.high_water_gen == 3

    for gen, jti in ((3, "j3-again"), (2, "j2")):
        with pytest.raises(L.AuthStaleError) as got:
            await repo.stage_pending(pod, _cred(uid="uid-A", epoch=1, gen=gen, jti=jti), AUTH_TTL)
        assert got.value.code == errcode.ErrUnauthorized, f"gen={gen} 应被水位挡住"

    stored, _ = await repo.get_auth(pod)
    assert stored.pending.jti == "j3", "被拒的重签污染了当前 pending"
    assert stored.high_water_gen == 3


async def test_stage_pending_also_refuses_anything_not_above_the_current_active(rdb) -> None:
    """★ 第二道门:`gen > active.gen`。与水位门**同时**成立才放行。

    水位门治"计数器复位后的号码复用",这道门治"把一个比当前 active 还旧的凭据
    暂存进来"。少了它,一次乱序的对账就能让 DS 拿到一张代际倒退的 pending,
    首个心跳一来就把 active 往回推。

    ★ 变异:删掉 `stage_pending` 里
      `if rec.HasField("active") and cred.gen <= rec.active.gen: raise` → 本条红。
      (构造上刻意把 high_water 抬到 active 之上,让水位门放行、只剩这道门。)
    """
    pod = "a07-activegate"
    repo, active_cred, _ = await _activate_ready(rdb, pod, gen=7)
    assert active_cred.gen == 7

    # 先把水位抬到 9:此时 gen=8 已经不受水位门管辖,只可能被 active 门拦住。
    # 直接改记录是为了把两道门**拆开**测;走正常链路的话两道门会一起生效,
    # 删掉任意一条判据用例都还是绿的。
    rec, _ = await repo.get_auth(pod)
    rec.high_water_gen = 5
    await rdb.set(L.auth_key(pod), rec.SerializeToString(), px=int(AUTH_TTL * 1000))

    with pytest.raises(L.AuthStaleError):
        await repo.stage_pending(pod, _cred(uid="uid-A", epoch=1, gen=6, jti="j6"), AUTH_TTL)
    stored, _ = await repo.get_auth(pod)
    assert not stored.HasField("pending"), "低于 active 的 gen 不得留下 pending"
    assert stored.active.gen == 7


async def test_stage_pending_stores_a_clone_not_the_callers_object(rdb) -> None:
    """★ 落库的是**克隆**:调用方在返回后继续改那个 proto,已落盘字节不许跟着变。

    签发器复用同一个 message 缓冲区是常见写法。存指针的话,下一轮签发改字段会
    "追溯性地"改掉上一代已落库的 pending —— Redis 里的凭据与 DS 手上那张分裂,
    而没有任何一次写操作留下痕迹。

    ★ 变异:把 `stage_pending` 里的 `rec.pending.CopyFrom(cred)` 换成把 cred 直接
      挂进记录(如共享同一个 message 对象)→ 本条红。
    """
    pod = "a08-clone"
    repo = hauth.RedisHubAuthRepo(rdb)
    await repo.init_auth(pod, "uid-A", AUTH_TTL)
    cred = _cred(uid="uid-A", epoch=1, gen=3, jti="j3")
    await repo.stage_pending(pod, cred, AUTH_TTL)

    cred.jti = "j3-MUTATED-AFTER-STAGE"
    cred.token_sha256 = "sha-MUTATED"
    stored, _ = await repo.get_auth(pod)
    assert stored.pending.jti == "j3"
    assert stored.pending.token_sha256 == "sha-j3"


async def test_stage_pending_refuses_every_kind_of_incomplete_credential(rdb) -> None:
    """★ 缺任一识别字段的凭据一律 ErrInvalidArg —— 不许存"半张票"。

    少一个字段,后续的 `cred_matches` 就只能降级成"比 gen",而 gen 会复用。
    逐字段单列(而不是"全空一起测")是因为合并成一条时,只要还剩**任意一个**
    完整性判据,用例就照样绿 —— 而被删掉的那个正是被打穿的门。

    `writer_epoch=0` 单独归 AuthStale(ErrUnauthorized)而不是 ErrInvalidArg:
    它不是"字段没填",是"这份凭据属于另一代 writer",调用方的处置完全不同
    (重走 bootstrap vs 修请求)。

    ★ 变异:从 `validate_stored_credential` 的判据链里删掉任意一项(如 `cred.kid == ""`)
      → 对应那条子用例红。
    """
    pod = "a09-incomplete"
    repo = hauth.RedisHubAuthRepo(rdb)
    await repo.init_auth(pod, "uid-A", AUTH_TTL)

    invalid_arg = {
        "missing_gen": lambda c: setattr(c, "gen", 0),
        "missing_jti": lambda c: setattr(c, "jti", ""),
        "missing_uid": lambda c: setattr(c, "instance_uid", ""),
        "missing_epoch": lambda c: setattr(c, "protocol_epoch", 0),
        "missing_kid": lambda c: setattr(c, "kid", ""),
        "missing_hash": lambda c: setattr(c, "token_sha256", ""),
        "missing_exp": lambda c: setattr(c, "exp_ms", 0),
        "already_expired": lambda c: setattr(c, "exp_ms", 1),
    }
    for name, mutate in invalid_arg.items():
        cred = _cred(uid="uid-A", epoch=1, gen=11, jti="j11")
        mutate(cred)
        with pytest.raises(errcode.PandoraError) as got:
            await repo.stage_pending(pod, cred, AUTH_TTL)
        assert got.value.code == errcode.ErrInvalidArg, name

    legacy_writer = _cred(uid="uid-A", epoch=1, gen=11, jti="j11")
    legacy_writer.writer_epoch = WRITER - 1
    with pytest.raises(L.AuthStaleError) as stale:
        await repo.stage_pending(pod, legacy_writer, AUTH_TTL)
    assert stale.value.code == errcode.ErrUnauthorized

    stored, found = await repo.get_auth(pod)
    assert found and not stored.HasField("pending"), "任何一条被拒的 stage 都不许留下 pending"


# ── ③ 四元组:四个字段各一条,绝不合并 ───────────────────────────────────────
#
# `(instance_uid, protocol_epoch, gen, jti)` 才是凭据身份(allocator.proto 的
# HubDSCredential 注释明写)。合并成"全不匹配"一条的话,只要**任意一个**比较
# 还在,用例就照样绿 —— 而被删掉的那个正是被打穿的门。所以四个字段逐个立案,
# 每条只动一个字段,其余三个保持与已激活凭据逐字相等。
#
# 判据分布在两处:uid / protocol_epoch 由 activate_heartbeat 步骤 ③ 的**记录级**
# 比较拦下,gen / jti 由步骤 ④ 的 `cred_matches` 拦下。四条都必须 fail-closed
# 且**零副作用**(分片镜像一个字节都不许动)。


def _tuple_variant(base: hauth.CredentialIdentity, **overrides) -> hauth.CredentialIdentity:
    """只改一个字段的身份变体。"""
    fields = {
        "gen": base.gen,
        "jti": base.jti,
        "instance_uid": base.instance_uid,
        "protocol_epoch": base.protocol_epoch,
        "token_sha256": base.token_sha256,
        "kid": base.kid,
        "writer_epoch": base.writer_epoch,
    }
    fields.update(overrides)
    return hauth.CredentialIdentity(**fields)


async def _assert_heartbeat_rejected_without_side_effects(rdb, repo, pod, ident) -> None:
    before = await rdb.get(L.shard_key(pod))
    before_auth = await rdb.get(L.auth_key(pod))
    with pytest.raises(L.AuthStaleError) as got:
        await repo.activate_heartbeat(pod, ident, _hb(state="ready", players=2))
    assert got.value.code == errcode.ErrUnauthorized
    assert await rdb.get(L.shard_key(pod)) == before, "被拒的心跳改写了分片镜像"
    assert await rdb.get(L.auth_key(pod)) == before_auth, "被拒的心跳改写了授权记录"


async def test_heartbeat_rejects_a_mismatched_instance_uid(rdb) -> None:
    """★ 四元组之 ①instance_uid:换了 GameServer 实例就不是同一份凭据。

    同名 Pod 重建后 GameServer UID 会变。不比 uid 的话,旧实例手里的凭据能给
    **新实例**的心跳背书:新实例被判成"已激活",而它压根没走过投递流程 ——
    §9.22 要求的 exact 实例绑定当场失效,且两边日志全绿。

    ★ 变异:删掉 `activate_heartbeat` 步骤 ③ 里的
      `auth_rec.instance_uid != ident.instance_uid` → 本条红。
    """
    pod = "a10-uid"
    repo, _, ident = await _activate_ready(rdb, pod)
    await _assert_heartbeat_rejected_without_side_effects(
        rdb, repo, pod, _tuple_variant(ident, instance_uid="uid-REBUILT")
    )


async def test_heartbeat_rejects_a_mismatched_protocol_epoch(rdb) -> None:
    """★ 四元组之 ②protocol_epoch:实例轮次不同就不是同一次 bootstrap。

    epoch 是抗"代际计数器因 TTL 复位而回退"的那一层。只比 gen+jti 时,一次复位
    就能让旧 gen 重新变成"当前代际",迟到的旧心跳因此复活。

    ★ 变异:删掉 `activate_heartbeat` 步骤 ③ 里的
      `auth_rec.protocol_epoch != ident.protocol_epoch` → 本条红。
    """
    pod = "a11-epoch"
    repo, _, ident = await _activate_ready(rdb, pod)
    await _assert_heartbeat_rejected_without_side_effects(
        rdb, repo, pod, _tuple_variant(ident, protocol_epoch=ident.protocol_epoch + 1)
    )


async def test_heartbeat_rejects_a_lower_generation(rdb) -> None:
    """★ 四元组之 ③gen:代际是单调门,旧代际令牌永远不能顶掉当前 active。

    这条正是凭据吊销的执行面:重签一张新令牌后,泄露的旧令牌必须立刻失去
    背书能力。gen 比较是唯一在**心跳这一跳**上强制它的地方。

    ★ 变异:把 `ledger.cred_matches` 里的 `cred.gen != ident.gen` 删掉 → 本条红。
    """
    pod = "a12-gen"
    repo, _, ident = await _activate_ready(rdb, pod, gen=7)
    stale = _tuple_variant(ident, gen=3, jti="j3", token_sha256="sha-j3")
    # ★ 只改 gen 会让 jti/hash 仍指向当前 active,于是被 jti 判据先拦下 —— 那样
    #   删掉 gen 判据本条也还是绿的。要钉住 gen 这道门,必须让整份变体在语义上
    #   就是"另一代真实存在过的凭据"(gen/jti/hash 同时回退到第 3 代)。
    #   同理下一条只回退 jti+hash、保持 gen 不变。
    await _assert_heartbeat_rejected_without_side_effects(rdb, repo, pod, stale)


async def test_heartbeat_rejects_a_different_jti_within_the_same_generation(rdb) -> None:
    """★ 四元组之 ④jti:同 gen 也可能是两张不同的令牌。

    gen 由 INCR 发号,但计数器复位 / 并发重签都可能让同一个 gen 出现两次。
    jti 是那一刻唯一能区分它们的东西;不比 jti = 承认"gen 相同即同一张票",
    于是一张从未被 stage 过的伪造令牌只要猜对代际就能通过。

    ★ 变异:把 `ledger.cred_matches` 里的 `cred.jti != ident.jti` 删掉 → 本条红。
    """
    pod = "a13-jti"
    repo, _, ident = await _activate_ready(rdb, pod, gen=7)
    forged = _tuple_variant(ident, jti="j7-FORGED", token_sha256="sha-j7-FORGED")
    assert forged.gen == ident.gen, "本条必须只在 jti 维度上不同,gen 保持相等"
    await _assert_heartbeat_rejected_without_side_effects(rdb, repo, pod, forged)


async def test_heartbeat_rejects_a_credential_whose_only_drift_is_the_token_hash(rdb) -> None:
    """★ 四元组之外再加一层:kid / token_sha256 也必须逐字相等。

    四元组保证"是哪一张票",hash 保证"这张票的字节没被换过"。少了它,一个
    拿到 gen/jti(它们会进日志、进 annotation)的人就能自制一份身份糊过心跳。

    ★ 变异:把 `ledger.cred_matches` 最后一行改成 `return True` → 本条红。
    """
    pod = "a14-hash"
    repo, _, ident = await _activate_ready(rdb, pod)
    await _assert_heartbeat_rejected_without_side_effects(
        rdb, repo, pod, _tuple_variant(ident, token_sha256="sha-TAMPERED")
    )
    await _assert_heartbeat_rejected_without_side_effects(
        rdb, repo, pod, _tuple_variant(ident, kid="kid-ROTATED")
    )


# ── ④ 相位闸:白名单之外一律不下发凭据 ───────────────────────────────────────


async def test_only_bootstrap_active_rotating_phases_may_stage_a_credential(rdb) -> None:
    """★ 遍历**全部** phase:QUARANTINED / TERMINATING 一律拒 stage,其余放行。

    QUARANTINED 是"这台 DS 的凭据泄露了",TERMINATING 是"这台 DS 正在下线"。
    两者都是显式运维 tombstone,恢复只能走受控 purge/recreate;能 stage 进新凭据
    就等于任何一次普通 Fleet 对账都能把被隔离的 DS 悄悄拉回服务。

    ★ 用 `DESCRIPTOR.values` 遍历而不是手写子集:proto 以后新增 phase 时,手写
      子集会**静默漏测**那个新值,而新值默认落在"不锁定"一侧(=可分配)。
    ★ 断言消息用 `enum_name` 而不是裸 `.Name()` —— 未知枚举值会让后者在构造
      断言消息时先抛 ValueError,把一条清晰的失败变成不可读的内部错误。

    ★ 变异:把 `ledger.phase_locked` 改成只判 `HUB_AUTH_PHASE_QUARANTINED`
      → TERMINATING 子例红。
    """
    repo = hauth.RedisHubAuthRepo(rdb)
    locked = {hubpb.HUB_AUTH_PHASE_QUARANTINED, hubpb.HUB_AUTH_PHASE_TERMINATING}
    assert locked < set(ALL_PHASES), "白名单常量与生成物枚举已经漂移"

    for phase in ALL_PHASES:
        pod = f"a15-phase-{phase}"
        rec = hubpb.HubShardAuthStorageRecord(
            pod_name=pod,
            instance_uid="uid-A",
            protocol_epoch=1,
            phase=phase,
            required_writer_epoch=WRITER,
        )
        await rdb.set(L.auth_key(pod), rec.SerializeToString(), px=int(AUTH_TTL * 1000))
        cred = _cred(uid="uid-A", epoch=1, gen=4, jti="j4")
        name = enum_name(hubpb.HubAuthPhase, phase)
        if phase in locked:
            with pytest.raises(L.AuthStaleError):
                await repo.stage_pending(pod, cred, AUTH_TTL)
            stored, _ = await repo.get_auth(pod)
            assert not stored.HasField("pending"), f"{name} 相位不得留下 pending"
        else:
            staged = await repo.stage_pending(pod, cred, AUTH_TTL)
            assert staged.pending.gen == 4, name


async def test_only_active_and_rotating_phases_are_routable(rdb) -> None:
    """★ 可路由相位比可 stage 相位**更窄**:只有 ACTIVE / ROTATING。

    BOOTSTRAP 允许继续投递凭据(它就是在等首个心跳),但**绝不可路由** ——
    放行它等于把玩家送进一台还没证明自己活着的 DS。两个集合混成一个就会出这种事。

    ★ 变异:把 `ledger.phase_serving` 里加上 `HUB_AUTH_PHASE_BOOTSTRAP` → 本条红。
    """
    pod = "a16-serving"
    repo, _, _ = await _activate_ready(rdb, pod)
    ok = await repo.check_routable(pod, L.now_ms(), 30_000)
    assert ok.ok is True, ok.reason

    rec, _ = await repo.get_auth(pod)
    for phase in ALL_PHASES:
        rec.phase = phase
        await rdb.set(L.auth_key(pod), rec.SerializeToString(), px=int(AUTH_TTL * 1000))
        out = await repo.check_routable(pod, L.now_ms(), 30_000)
        name = enum_name(hubpb.HubAuthPhase, phase)
        if phase in (hubpb.HUB_AUTH_PHASE_ACTIVE, hubpb.HUB_AUTH_PHASE_ROTATING):
            assert out.ok is True, f"{name} 应可路由,却因 {out.reason} 被拒"
        else:
            assert out.ok is False, f"{name} 不得可路由"
            assert out.reason == "phase-not-active"


# ── ⑤ 轮换:active 与 pending 并存时的判定优先级 ────────────────────────────


async def test_rotation_keeps_the_old_active_serving_while_pending_waits(rdb) -> None:
    """★ ROTATING 期间 active 与 pending 并存:**旧 active 仍是幂等合法心跳**。

    这是不停机轮换的全部意义。若 stage 一份 pending 就让旧 active 立刻失效,
    那么从"投递 annotation"到"DS 读到并发首个新心跳"这几秒里,这台 Hub 会被判成
    stale → 移出可分配集 → 在场玩家的心跳全被拒。一次例行令牌续期变成一次掉线事故。

    断言三件事:相位翻 ROTATING、旧 active 心跳 accepted=False(幂等而非拒绝)、
    pending 原样保留等待它自己的首跳。

    ★ 变异:把 `activate_heartbeat` 步骤 ④ 的 active 分支(`promote = False`)
      改成 `raise AuthStaleError` → 本条红。
    """
    pod = "a17-rotating"
    repo, _, old_ident = await _activate_ready(rdb, pod, gen=7)
    new_cred = _cred(uid="uid-A", epoch=1, gen=8, jti="j8")
    staged = await repo.stage_pending(pod, new_cred, AUTH_TTL)
    assert staged.phase == hubpb.HUB_AUTH_PHASE_ROTATING, enum_name(
        hubpb.HubAuthPhase, staged.phase
    )

    out = await repo.activate_heartbeat(pod, old_ident, _hb(state="ready", players=1))
    assert out.accepted is False, "轮换期的旧 active 心跳是幂等,不是新的激活"
    assert out.active_gen == 7
    assert out.shard_found is True

    rec, _ = await repo.get_auth(pod)
    assert rec.active.gen == 7
    assert rec.pending.gen == 8, "旧 active 的心跳不得吞掉正在等待的 pending"
    assert rec.phase == hubpb.HUB_AUTH_PHASE_ROTATING


async def test_pending_heartbeat_wins_the_rotation_and_burns_the_old_active(rdb) -> None:
    """★ pending 的首个合法心跳是**唯一线性化点**:promote 之后旧 active 立即失效。

    promote 必须是一次性的、单向的。若旧 active 在 promote 之后还能被判合法,
    那么"轮换"就没有真正吊销任何东西 —— 泄露的旧令牌可以一直用到自己 exp 为止。

    ★ 变异:把 `activate_heartbeat` 步骤 ⑥ 里的
      `auth_rec.ClearField("pending")` 删掉 → 本条红(pending 残留,且相位没落 ACTIVE)。
    """
    pod = "a18-promote"
    repo, _, old_ident = await _activate_ready(rdb, pod, gen=7)
    new_cred = _cred(uid="uid-A", epoch=1, gen=8, jti="j8")
    await repo.stage_pending(pod, new_cred, AUTH_TTL)

    promoted = await repo.activate_heartbeat(pod, _ident(new_cred), _hb(state="ready", players=1))
    assert promoted.accepted is True
    assert promoted.active_gen == 8
    assert promoted.active_jti == "j8"

    rec, _ = await repo.get_auth(pod)
    assert rec.active.gen == 8
    assert not rec.HasField("pending"), "promote 后 pending 必须清空"
    assert rec.phase == hubpb.HUB_AUTH_PHASE_ACTIVE
    assert rec.delivered_rv == ""

    with pytest.raises(L.AuthStaleError):
        await repo.activate_heartbeat(pod, old_ident, _hb(state="ready", players=1))


async def test_promote_and_the_shard_flip_to_ready_land_in_one_transaction(rdb) -> None:
    """★ promote 与 warming→ready + active 元组投影必须**同一次 EXEC**。

    拆成两步就会出现「promote 成功但分片写失败 / 进程崩溃」的半激活:授权记录说
    这台 DS 是 active,分片镜像却还停在 warming —— 它既不可分配、又因为 active
    已占位而挡住下一次 stage,人工不介入永远恢复不了。

    ★ 变异:把 `activate_heartbeat` 步骤 ⑦ 里的
      `shard.last_verified_gen = auth_rec.active.gen` 改成保持不变 → 本条红
      (check_routable 会因 `shard-not-verified-by-active` 拒路由)。
    """
    pod = "a19-sametx"
    repo = hauth.RedisHubAuthRepo(rdb)
    await repo.init_auth(pod, "uid-A", AUTH_TTL)
    cred = _cred(uid="uid-A", epoch=1, gen=7, jti="j7")
    await repo.stage_pending(pod, cred, AUTH_TTL)
    await _seed_warming_shard(rdb, pod)

    out = await repo.activate_heartbeat(pod, _ident(cred), _hb(state="", players=3))
    assert out.accepted and out.shard_found
    assert out.shard_state == "ready"

    shard = await _shard(rdb, pod)
    assert shard.state == "ready"
    assert shard.last_verified_gen == 7
    assert shard.last_verified_jti == "j7"
    assert shard.gameserver_uid == "uid-A"
    assert shard.auth_epoch == 1
    assert shard.last_verified_writer_epoch == WRITER
    assert shard.reported_connected_count == 3
    assert shard.player_count == 0, "权威人数由账本派生,不接受心跳上报覆盖"

    # ★ 此刻**还不可路由**:实报 3 人 > 账本追踪的 0 个 connected ownership,
    #   `untracked-connected-players` 会挡住新分配(不能把实报回填成权威,低估会超发)。
    #   下一跳实报归零后才可路由 —— 这正好也证明 reported 计数是每跳刷新的。
    assert (await repo.check_routable(pod, L.now_ms(), 30_000)).reason == (
        "untracked-connected-players"
    )
    await repo.activate_heartbeat(pod, _ident(cred), _hb(state="", players=0))
    routable = await repo.check_routable(pod, L.now_ms(), 30_000)
    assert routable.ok is True, routable.reason


async def test_missing_shard_mirror_blocks_the_promote_entirely(rdb) -> None:
    """★ 分片镜像缺失时:**不 promote、不写任何键**,交 biz 先 reconcile 拓扑。

    这是上一条"同事务"的另一半。分片不存在却照样 promote,就会得到一条 active
    授权记录配一个不存在的分片 —— 后续任何一次 stage 都会被"已有 active"挡住,
    而这台 DS 永远不可路由。

    ★ 变异:把 `activate_heartbeat` 步骤 ⑤ 的 `if s_raw is None: ... return out`
      改成继续往下走 → 本条红。
    """
    pod = "a20-noshard"
    repo = hauth.RedisHubAuthRepo(rdb)
    await repo.init_auth(pod, "uid-A", AUTH_TTL)
    cred = _cred(uid="uid-A", epoch=1, gen=7, jti="j7")
    await repo.stage_pending(pod, cred, AUTH_TTL)

    out = await repo.activate_heartbeat(pod, _ident(cred), _hb(state="ready", players=1))
    assert out.shard_found is False
    assert out.accepted is False

    rec, _ = await repo.get_auth(pod)
    assert rec.pending.gen == 7, "分片缺失必须保住 pending 供下轮重试"
    assert not rec.HasField("active")
    assert rec.phase == hubpb.HUB_AUTH_PHASE_BOOTSTRAP


async def test_quarantined_phase_never_flips_a_shard_to_ready(rdb) -> None:
    """★ 已隔离的 DS 心跳一律拒,且分片停在 warming。

    紧急吊销之后这台 DS 手里的令牌可能已经泄露。让它靠一次心跳把自己翻 ready,
    等于吊销从未发生。

    ★ 变异:把 `activate_heartbeat` 步骤 ② 的 `if L.phase_locked(...)` 删掉 → 本条红。
    """
    pod = "a21-quarantine"
    repo = hauth.RedisHubAuthRepo(rdb)
    await repo.init_auth(pod, "uid-A", AUTH_TTL)
    cred = _cred(uid="uid-A", epoch=1, gen=7, jti="j7")
    await repo.stage_pending(pod, cred, AUTH_TTL)
    await _seed_warming_shard(rdb, pod)

    rec, _ = await repo.get_auth(pod)
    rec.phase = hubpb.HUB_AUTH_PHASE_QUARANTINED
    await rdb.set(L.auth_key(pod), rec.SerializeToString(), px=int(AUTH_TTL * 1000))

    with pytest.raises(L.AuthStaleError):
        await repo.activate_heartbeat(pod, _ident(cred), _hb(state="ready", players=1))
    assert (await _shard(rdb, pod)).state == "warming"


async def test_heartbeat_without_an_auth_record_is_unauthorized(rdb) -> None:
    """★ 压根没有授权记录时不得"顺手建一条"。

    自建 = 任何知道 pod 名的人都能把一台机器变成合法 DS。授权记录只能由
    Fleet 对账侧的 InitAuth 建立。

    ★ 变异:把 `activate_heartbeat` 步骤 ① 的 `if a_raw is None: raise` 改成建记录
      → 本条红。
    """
    repo = hauth.RedisHubAuthRepo(rdb)
    cred = _cred(uid="uid-A", epoch=1, gen=1, jti="j1")
    with pytest.raises(L.AuthStaleError) as got:
        await repo.activate_heartbeat("a22-ghost", _ident(cred), _hb(state="ready"))
    assert got.value.code == errcode.ErrUnauthorized
    assert await rdb.get(L.auth_key("a22-ghost")) is None


# ── ⑥ 心跳负载:时间与容量都不信 DS ─────────────────────────────────────────


async def test_heartbeat_freshness_uses_server_time_not_the_reported_timestamp(rdb) -> None:
    """★ 权威心跳时刻**只取服务端接收时间**,请求里的 ts_ms 只作遥测。

    采信 DS 上报时间的话,一台失联(或被攻破)的 DS 只要报一个未来时间戳,就能
    让自己永远"心跳新鲜" —— 心跳超时这条补偿(不变量 §9.4)彻底失效,玩家被
    持续分配到一台已经不在的机器上。

    构造:上报一个 +1 小时的未来 ts,然后按**服务端时钟 + maxAge** 判定;
    若 ts_ms 被采信,`heartbeat-stale` 就不会出现。

    ★ 变异:把 `activate_heartbeat` 里的
      `auth_rec.last_active_heartbeat_ms = server_now_ms` 改成 `= inp.ts_ms` → 本条红。
    """
    pod = "a23-clock"
    repo = hauth.RedisHubAuthRepo(rdb)
    await repo.init_auth(pod, "uid-A", AUTH_TTL)
    cred = _cred(uid="uid-A", epoch=1, gen=7, jti="j7")
    await repo.stage_pending(pod, cred, AUTH_TTL)
    await _seed_warming_shard(rdb, pod)

    future_ts = L.now_ms() + 3_600_000
    out = await repo.activate_heartbeat(
        pod, _ident(cred), _hb(state="ready", ts_ms=future_ts, players=0)
    )
    assert out.accepted

    rec, _ = await repo.get_auth(pod)
    assert rec.last_active_heartbeat_ms < future_ts, "上报时间戳被当成了权威心跳时刻"

    max_age = 30_000
    stale = await repo.check_routable(pod, L.now_ms() + max_age + 1_000, max_age)
    assert stale.ok is False
    assert stale.reason == "heartbeat-stale"


async def test_heartbeat_max_players_must_equal_the_allocator_capacity(rdb) -> None:
    """★ DS 自报 MaxPlayers 与 allocator capacity **必须精确相等**,不等就整条拒。

    两边不等说明"这台机器能装几个人"的认知已经分叉:继续往下走会按错的上限发座位
    —— 账本说还有座、DS 直接拒连,玩家反复"进大厅失败"而全链日志绿的。
    而且这道闸必须在 promote / 心跳时刻 / ledger 清理**任何副作用之前**。

    ★ 变异:把 `activate_heartbeat` 里的
      `if shard.capacity <= 0 or inp.max_players != shard.capacity: raise` 删掉 → 本条红。
    """
    pod = "a24-capacity"
    repo = hauth.RedisHubAuthRepo(rdb)
    await repo.init_auth(pod, "uid-A", AUTH_TTL)
    cred = _cred(uid="uid-A", epoch=1, gen=7, jti="j7")
    await repo.stage_pending(pod, cred, AUTH_TTL)
    await _seed_warming_shard(rdb, pod, capacity=500)

    with pytest.raises(errcode.PandoraError) as got:
        await repo.activate_heartbeat(pod, _ident(cred), _hb(capacity=499, state="ready"))
    assert got.value.code == errcode.ErrInvalidState

    rec, _ = await repo.get_auth(pod)
    assert rec.pending.gen == 7, "容量不符必须在 promote 之前拦下"
    assert not rec.HasField("active")
    assert (await _shard(rdb, pod)).state == "warming"


async def test_heartbeat_rejects_a_self_contradictory_player_roster(rdb) -> None:
    """★ player_ids 与 player_count 不符 / 含 0 / 含重复 → ErrInvalidArg。

    这三种都不是"数字不准",是上报本身坏了。放行的话账本会按一份自相矛盾的名单
    做差集,清理出的结果无法解释;而 §9.6 明确要求 DS 只报事实、服务端校验事实。

    ★ 变异:删掉 `activate_heartbeat` 里的
      `if player_id in seen: raise`(或 `len(inp.player_ids) != inp.player_count`)
      → 对应子例红。
    """
    pod = "a25-roster"
    repo, _, ident = await _activate_ready(rdb, pod)
    bad_inputs = {
        "count_mismatch": hauth.ActivateHeartbeatInput(
            player_count=2,
            player_ids=(1,),
            max_players=500,
            auth_ttl_sec=AUTH_TTL,
            shard_ttl_sec=SHARD_TTL,
        ),
        "zero_player_id": hauth.ActivateHeartbeatInput(
            player_count=2,
            player_ids=(1, 0),
            max_players=500,
            auth_ttl_sec=AUTH_TTL,
            shard_ttl_sec=SHARD_TTL,
        ),
        "duplicate_player_id": hauth.ActivateHeartbeatInput(
            player_count=2,
            player_ids=(9, 9),
            max_players=500,
            auth_ttl_sec=AUTH_TTL,
            shard_ttl_sec=SHARD_TTL,
        ),
    }
    for name, inp in bad_inputs.items():
        with pytest.raises(errcode.PandoraError) as got:
            await repo.activate_heartbeat(pod, ident, inp)
        assert got.value.code == errcode.ErrInvalidArg, name


async def test_auth_key_ttl_is_independent_of_the_shorter_shard_ttl(rdb) -> None:
    """★ 授权键用 auth_ttl,分片键用 shard_ttl —— 两者绝不能共用一个值。

    拿较短的 shard_ttl 去写授权键,授权记录会**先于**分片消失;DS 的下一跳因此
    找不到授权记录而被判 stale,一台健康的 Hub 被踢出可分配集,在场玩家掉线。

    ★ 变异:把 `activate_heartbeat` 结尾的
      `pipe.set(a_key, auth_payload, px=int(inp.auth_ttl_sec * 1000))` 改成用
      `inp.shard_ttl_sec` → 本条红。
    """
    pod = "a26-ttl"
    await _activate_ready(rdb, pod)
    auth_pttl = await rdb.pttl(L.auth_key(pod))
    shard_pttl = await rdb.pttl(L.shard_key(pod))
    assert auth_pttl > SHARD_TTL * 1000, "授权键 TTL 被分片 TTL 缩短了"
    assert shard_pttl <= SHARD_TTL * 1000


# ── ⑦ MarkDelivered:只绑定当前 pending ─────────────────────────────────────


async def test_mark_delivered_binds_the_exact_pending_tuple(rdb) -> None:
    """★ 迟到的旧 PATCH 响应不得把 delivered_rv 写到更高代际的 pending 上。

    写上去 = 投递侧以为新代际已经送达,于是停止重投。DS 手里还是旧令牌,
    Redis 里是新 pending,两边永远追不上,而所有日志都是 2xx 全绿。

    三段:正常记 rv → 高代际替换后旧响应迟到必须拒 → 同 gen/jti 但 kid 不同也不是同一份。

    ★ 变异:把 `mark_delivered` 里的 `not L.stored_credential_equal(rec.pending, expected)`
      改成只比 `rec.pending.gen != expected.gen` → 第三段红。
    """
    pod = "a27-delivered"
    repo = hauth.RedisHubAuthRepo(rdb)
    await repo.init_auth(pod, "uid-A", AUTH_TTL)
    g5 = _cred(uid="uid-A", epoch=1, gen=5, jti="j5")
    await repo.stage_pending(pod, g5, AUTH_TTL)
    await repo.mark_delivered(pod, g5, "rv-5", AUTH_TTL)
    rec, found = await repo.get_auth(pod)
    assert found and rec.delivered_rv == "rv-5"

    g6 = _cred(uid="uid-A", epoch=1, gen=6, jti="j6")
    await repo.stage_pending(pod, g6, AUTH_TTL)
    with pytest.raises(L.AuthStaleError):
        await repo.mark_delivered(pod, g5, "rv-old-late", AUTH_TTL)
    rec, _ = await repo.get_auth(pod)
    assert rec.pending.gen == 6
    assert rec.delivered_rv == "", "迟到的旧响应污染了新 pending 的投递状态"

    wrong_kid = _cred(uid="uid-A", epoch=1, gen=6, jti="j6", kid="other-kid")
    with pytest.raises(L.AuthStaleError):
        await repo.mark_delivered(pod, wrong_kid, "rv-wrong", AUTH_TTL)
    rec, _ = await repo.get_auth(pod)
    assert rec.delivered_rv == ""


async def test_mark_delivered_requires_a_resource_version(rdb) -> None:
    """★ 空 rv 直接 ErrInvalidArg:空串证明不了"投递到哪个对象版本"。

    允许空 rv,delivered 就退化成一个布尔标记,后续再也无法判断"annotation 上
    这一份到底是不是我投的那一份"。

    ★ 变异:删掉 `mark_delivered` 里的 `if rv == "": raise` → 本条红。
    """
    pod = "a28-emptyrv"
    repo = hauth.RedisHubAuthRepo(rdb)
    await repo.init_auth(pod, "uid-A", AUTH_TTL)
    cred = _cred(uid="uid-A", epoch=1, gen=5, jti="j5")
    await repo.stage_pending(pod, cred, AUTH_TTL)
    with pytest.raises(errcode.PandoraError) as got:
        await repo.mark_delivered(pod, cred, "", AUTH_TTL)
    assert got.value.code == errcode.ErrInvalidArg


# ── ⑧ Quarantine:紧急吊销 ───────────────────────────────────────────────────


async def test_quarantine_refuses_a_blind_pod_scoped_revocation(rdb) -> None:
    """★ 不接受"按 pod 名盲吊销":必须提交当前完整 active 身份,否则零副作用返回。

    盲吊销会误伤**同名重建后的新** GameServer:一条几分钟前发出的运维请求,
    在新实例已经接客之后到达,把一台健康 Hub 连同上面的玩家一起 drain 掉。

    ★ 变异:把 `quarantine_expected` 里的 `not L.cred_matches(active, expected)`
      判据删掉 → 本条红。
    """
    pod = "a29-blind"
    repo, cred, ident = await _activate_ready(rdb, pod)
    before_auth = await rdb.get(L.auth_key(pod))
    before_shard = await rdb.get(L.shard_key(pod))

    wrong = _tuple_variant(ident, jti="stale", token_sha256="sha-stale")
    got = await repo.quarantine_expected(pod, wrong, AUTH_TTL, SHARD_TTL)
    assert got.auth_quarantined is False
    assert got.projection_drained is False
    assert await rdb.get(L.auth_key(pod)) == before_auth
    assert await rdb.get(L.shard_key(pod)) == before_shard


async def test_quarantine_writes_a_persistent_tombstone_that_init_cannot_revive(rdb) -> None:
    """★ tombstone **无 TTL**,且之后任何 uid 的 InitAuth 都复活不了它。

    带 TTL 的 tombstone 会在 allocator 停机后自动消失,于是那台仍然活着、
    仍握着泄露令牌的 GameServer 可以被重新 Init/Stage —— 吊销静默失效。
    同名重建(新 uid)也不行:恢复只能走显式受控的 purge/recreate。

    ★ 变异:把 `quarantine_expected` 里的 `pipe.set(a_key, auth_payload)` 改成带
      `px=int(auth_ttl_sec * 1000)` → 本条红。
    """
    pod = "a30-tombstone"
    repo, _, ident = await _activate_ready(rdb, pod)
    got = await repo.quarantine_expected(pod, ident, AUTH_TTL, SHARD_TTL)
    assert got.auth_quarantined is True
    assert got.projection_drained is True

    assert await rdb.pttl(L.auth_key(pod)) == -1, "吊销 tombstone 必须是持久键"
    rec, _ = await repo.get_auth(pod)
    assert rec.phase == hubpb.HUB_AUTH_PHASE_QUARANTINED
    assert not rec.HasField("pending")
    shard = await _shard(rdb, pod)
    assert shard.state == "draining"
    assert shard.draining_since_ms > 0

    frozen = await rdb.get(L.auth_key(pod))
    for uid in ("uid-A", "uid-NEW"):
        with pytest.raises(L.AuthStaleError):
            await repo.init_auth(pod, uid, AUTH_TTL)
    assert await rdb.get(L.auth_key(pod)) == frozen
    assert await rdb.pttl(L.auth_key(pod)) == -1

    routable = await repo.check_routable(pod, L.now_ms(), 30_000)
    assert routable.ok is False


async def test_quarantine_still_revokes_when_the_shard_projection_has_drifted(rdb) -> None:
    """★ 分片镜像已漂移时:**照样吊销授权**,只是 drain 不算成功。

    两个布尔分开就是为了这一刻 —— 泄露的凭据必须先失效;派生投影 drain 是次要的。
    把 `projection_drained` 反过来当成吊销的前置条件,一次无关的分片漂移就能让
    紧急吊销整条失败。

    ★ 变异:在 `quarantine_expected` 里把 `if projection_matches:` 提升成"不匹配
      就直接 return 空结果" → 本条红。
    """
    pod = "a31-drift"
    repo, _, ident = await _activate_ready(rdb, pod)
    drifted = await _shard(rdb, pod)
    drifted.gameserver_uid = "uid-REBUILT"
    await rdb.set(L.shard_key(pod), L.marshal_shard(drifted), px=int(SHARD_TTL * 1000))
    shard_before = await rdb.get(L.shard_key(pod))

    got = await repo.quarantine_expected(pod, ident, AUTH_TTL, SHARD_TTL)
    assert got.auth_quarantined is True, "投影漂移绝不能阻止凭据吊销"
    assert got.projection_drained is False
    assert await rdb.get(L.shard_key(pod)) == shard_before, "漂移的分片不得被改写"
    rec, _ = await repo.get_auth(pod)
    assert rec.phase == hubpb.HUB_AUTH_PHASE_QUARANTINED


# ── ⑨ 只读路径与墓碑方法 ────────────────────────────────────────────────────


async def test_check_routable_takes_no_seat_and_changes_nothing(rdb) -> None:
    """★ 只读可路由检查必须零变更(不占座、不刷 TTL、不 prune)。

    幂等重签 / 复用已有归属会高频调它。任何一处"顺手写"都会把一次只读观测变成
    写操作,而观测者拿的往往是旧快照(§9.22「状态优先查询唯一权威」)。

    ★ 变异:在 `check_routable` 的事务里把 `pipe.get(...)` 换成任意写命令
      (如 `pipe.expire(a_key, 60)`)→ 本条红。
    """
    pod = "a32-readonly"
    repo, _, _ = await _activate_ready(rdb, pod)
    auth_before = await rdb.get(L.auth_key(pod))
    shard_before = await rdb.get(L.shard_key(pod))
    shard_ttl_before = await rdb.pttl(L.shard_key(pod))

    out = await repo.check_routable(pod, L.now_ms(), 30_000)
    assert out.ok is True
    assert out.active_gen == 7
    assert out.instance_uid == "uid-A"
    assert out.capacity == 500
    assert out.hub_addr == "10.0.0.1:7777"
    assert out.release_track == "stable", "旧记录的空轨道必须 additive 迁移成 stable"

    assert await rdb.get(L.auth_key(pod)) == auth_before
    assert await rdb.get(L.shard_key(pod)) == shard_before
    assert abs(await rdb.pttl(L.shard_key(pod)) - shard_ttl_before) < 3_000


async def test_check_routable_reports_the_exact_reason_for_each_gate(rdb) -> None:
    """★ 每道路由闸都有**自己的** reason 串 —— 运维就是照着这些字符串定位的。

    合并成一句 "not routable" 的话,「分片没被当前 active 投影」与「实报人数超过
    账本追踪数」会长得一模一样,而它们的处置一个是等下一跳投影、一个是查
    admission 账本漏登记。

    ★ 关于 `shard-instance-mismatch` / `shard-writer-epoch-mismatch`:它们在
      `_routable_snapshot` 里写着,但**当前实现下不可达** —— 上游
      `modelb_routable_reason` 已经把 uid / epoch / writer_epoch 一并算进
      `shard-not-verified-by-active` 了。这是**照抄 Go**的结果(Go 的 routable()
      同样在 modelBRoutableReason 之后重复判一遍),所以本用例断言的是**实际**
      产出的那个串,而不是按代码行号猜的那个。把它们当可达来断言等于把测试写成
      「跟着 Python 的死代码走」,Go 侧一改就两边都发现不了。

    ★ 变异:把 `_routable_snapshot` 里的 `release_track` 校验删掉 → 本条红。
    """
    pod = "a33-reasons"
    repo, _, _ = await _activate_ready(rdb, pod)
    at = L.now_ms()
    original = await rdb.get(L.shard_key(pod))

    async def _reason_after(mutate) -> str:
        await rdb.set(L.shard_key(pod), original, px=int(SHARD_TTL * 1000))
        shard = await _shard(rdb, pod)
        mutate(shard)
        await rdb.set(L.shard_key(pod), L.marshal_shard(shard), px=int(SHARD_TTL * 1000))
        out = await repo.check_routable(pod, at, 30_000)
        assert out.ok is False
        return out.reason

    cases = {
        "shard-not-verified-by-active": lambda s: setattr(s, "last_verified_gen", 999),
        "shard-not-ready": lambda s: setattr(s, "state", "draining"),
        "shard-release-track-invalid": lambda s: setattr(s, "release_track", "beta"),
        "untracked-connected-players": lambda s: setattr(s, "reported_connected_count", 3),
        "max-players-mismatch": lambda s: setattr(s, "reported_max_players", 499),
    }
    for want, mutate in cases.items():
        assert await _reason_after(mutate) == want

    # uid 漂移**也**落 shard-not-verified-by-active(见上面的说明),而不是
    # shard-instance-mismatch —— 钉住它,免得有人"顺手修"成后者时无人发现。
    assert await _reason_after(lambda s: setattr(s, "gameserver_uid", "uid-REBUILT")) == (
        "shard-not-verified-by-active"
    )

    await rdb.set(L.shard_key(pod), original, px=int(SHARD_TTL * 1000))
    await rdb.delete(L.shard_key(pod))
    missing = await repo.check_routable(pod, at, 30_000)
    assert missing.ok is False and missing.reason == "shard-missing"

    await rdb.delete(L.auth_key(pod))
    gone = await repo.check_routable(pod, at, 30_000)
    assert gone.ok is False and gone.reason == "auth-missing"


async def test_integer_seat_reservation_stays_disabled(rdb) -> None:
    """★ 整数 seat++ 路径是一块**墓碑**:调用即炸,不许悄悄复活。

    整数座位与逐 assignment reservation 并存时,同一个玩家会占两个座位,而退座
    路径不对称(一边按人删、一边按数减),账本永远对不平。留着这个会抛错的方法
    比删掉安全 —— 删掉的话调用方找不到就自己再写一个整数计数器。

    ★ 变异:把 `reserve_routable_seat` 改成转发到 `check_routable` → 本条红。
    """
    repo = hauth.RedisHubAuthRepo(rdb)
    with pytest.raises(errcode.PandoraError) as got:
        await repo.reserve_routable_seat("a34-tombstone-method", L.now_ms(), 30_000, SHARD_TTL)
    assert got.value.code == errcode.ErrInvalidState


async def test_get_auth_refuses_to_guess_at_corrupted_bytes(rdb) -> None:
    """★ 授权记录解码失败 → ErrInvalidState,不许当成"没有记录"。

    当成 not-found 的话,下一步就是 InitAuth 新建一条 —— 一条坏掉的权威记录被
    一次读失败悄悄替换掉,原本绑定的实例身份、gen 水位、tombstone 全部丢失。

    ★ 变异:把 `get_auth` 里 `except Exception: raise ...` 改成 `return None, False`
      → 本条红。
    """
    pod = "a35-corrupt"
    await rdb.set(L.auth_key(pod), b"\xff\xff\xff\xff not-a-proto")
    repo = hauth.RedisHubAuthRepo(rdb)
    with pytest.raises(errcode.PandoraError) as got:
        await repo.get_auth(pod)
    assert got.value.code == errcode.ErrInvalidState
