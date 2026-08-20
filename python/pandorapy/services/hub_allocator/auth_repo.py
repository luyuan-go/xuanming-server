"""Model B「Redis 唯一授权权威」的授权记录数据层 —— 对应 Go 侧
`services/battle/hub_allocator/internal/data/hub_auth_repo.go` +
`hub_authoritative.go`(decision-revisit-ds-callback-auth §7)。

Redis key 模板:

    pandora:hub:auth:{<hub_pod_name>}  → HubShardAuthStorageRecord proto bytes

与分片镜像 `pandora:hub:shard:{pod}`、容量账本六键、写者水位 `pandora:hub:wfence:{pod}`
同 `{pod}` hashtag,保证同 slot 可事务。所有状态迁移走 WATCH/MULTI/EXEC 乐观锁
(与 hub_repo 一致,不用 Lua,便于 fakeredis 测试)。

★ read-modify-write 用默认的 `ParseFromString`(不 DiscardUnknown,不变量 §17):
  滚动更新期旧副本回写不得静默丢新字段。

═══ 状态机(五个转移,全部 fail-closed)═══

    InitAuth      不存在 → BOOTSTRAP(绑 uid,epoch=1,high_water_gen=0)
                  uid 变了 → 复位:清 active/pending,epoch++,high_water 保留(单调)
                  uid 相同 → 幂等不动
    StagePending  暂存 pending;要求 gen > high_water_gen 且 > active.gen
    MarkDelivered 记 pending 已 PATCH 投递到某 GameServer resourceVersion
    Activate      DS 首个合法心跳 = **唯一线性化点**:promote pending→active
                  + 应用心跳到分片镜像 + 投影 active 元组,**同一个 EXEC**
    Quarantine    紧急吊销(必须提交当前完整 active 身份,不接受按 pod 名盲吊销)

★ 为什么 promote 与分片写必须同事务:拆成两步就会出现「promote 成功但分片写失败 /
  进程崩溃」的**半激活** —— 授权记录说这台 DS 是 active,分片镜像却还停在 warming,
  于是它既不可分配、又挡住了下一次 stage,人工不介入就恢复不了(审核二轮 CE4)。
  分片镜像不存在(孤儿 / 早于拓扑种子)时刻意**不 promote、不写任何键**,
  交 biz 先 reconcile 拓扑再重试,保证 promote 与 warming→ready 恒同事务。

═══ 与 ledger.py 的分层 ═══

本模块 import `ledger.py`(下层:keys / fence / CAS 退避 / shard 编解码 / 凭据谓词 /
容量账本),`RedisHubAuthRepo` 继承 `ledger.HubCapacityLedgerOps`,于是 Python 侧的
方法集与 Go 的 `*RedisHubAuthRepo` 一致 —— Go 是同一个 struct 上的方法,只是分了文件。
"""

from __future__ import annotations

import dataclasses
from typing import Any

from pandora.hub.v1 import allocator_pb2 as hubpb
from redis.exceptions import WatchError

from pandorapy import errcode
from pandorapy.auth import DS_AUTH_WRITER_EPOCH_V2
from pandorapy.services.hub_allocator import ledger as L
from pandorapy.services.hub_allocator.ledger import (  # 直接转出,便于调用方只 import 本模块
    AuthStaleError,
    CredentialIdentity,
    WriterFence,
    WriterSupersededError,
)

__all__ = [
    "ActivateHeartbeatInput",
    "ActivateResult",
    "AuthStaleError",
    "CredentialIdentity",
    "QuarantineResult",
    "RedisHubAuthRepo",
    "WriterFence",
    "WriterSupersededError",
]


# ── 心跳入参 / 结果(Go: hub_authoritative.go)────────────────────────────────


@dataclasses.dataclass(slots=True)
class ActivateHeartbeatInput:
    """ActivateHeartbeat 的心跳负载。Go: ActivateHeartbeatInput。

    与凭据身份分开,便于 biz 组装。`ts_ms` 只作遥测 —— 见 activate_heartbeat 里
    为什么权威时刻只取服务端接收时间。
    """

    player_count: int = 0
    player_ids: tuple[int, ...] = ()
    max_players: int = 0
    state: str = ""
    ts_ms: int = 0
    # ★ 两个 TTL 必须分开(审核 CE8):授权键用 auth_ttl,分片键用 shard_ttl。
    #   用 shard_ttl 缩短授权键寿命会让授权记录先于分片消失,DS 下一跳被判 stale。
    auth_ttl_sec: float = 0.0
    shard_ttl_sec: float = 0.0


@dataclasses.dataclass(slots=True)
class ActivateResult:
    """Go: ActivateResult。"""

    # accepted:本次心跳完成了 pending→active 原子激活(线性化点首个合法心跳)。
    accepted: bool = False
    # shard_found=False → 分片镜像缺失(孤儿 / 早于拓扑种子):**未 promote、未写任何键**,
    # biz 需先 reconcile 拓扑再重试。
    shard_found: bool = False
    shard_state: str = ""
    active_gen: int = 0
    active_jti: str = ""
    instance_uid: str = ""
    protocol_epoch: int = 0
    writer_epoch: int = 0


@dataclasses.dataclass(slots=True)
class QuarantineResult:
    """Go: QuarantineResult。

    ★ 两个布尔分开的理由:`projection_drained=False` 只表示 shard 缺失 / 漂移,
      **绝不能**反过来阻止吊销 —— 泄露的凭据必须先失效,派生投影 drain 是次要的。
    """

    auth_quarantined: bool = False
    projection_drained: bool = False


class RedisHubAuthRepo(L.HubCapacityLedgerOps):
    """Model B 授权记录 + 容量账本的 Redis 实现。Go: data.RedisHubAuthRepo。"""

    __slots__ = ()

    # ── 只读 ─────────────────────────────────────────────────────────────────

    async def get_auth(self, pod: str) -> tuple[Any | None, bool]:
        """读授权记录。not found → (None, False)。Go: GetAuth。"""
        raw = await self._rdb.get(L.auth_key(pod))
        if raw is None:
            return None, False
        rec = hubpb.HubShardAuthStorageRecord()
        try:
            rec.ParseFromString(bytes(raw))
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInvalidState, "unmarshal hub auth %s: %s", pod, exc
            ) from exc
        return rec, True

    # ── InitAuth ─────────────────────────────────────────────────────────────

    async def init_auth(self, pod: str, instance_uid: str, auth_ttl_sec: float):
        """确保授权记录存在并绑定当前 DS 实例身份。Go: InitAuth。

        ★ 换实例(uid 变了)时 `protocol_epoch++` 而 `high_water_gen` **保留**:
          前者抗「代际计数器被 TTL 复位后重放旧 gen」,后者保证 gen 水位单调不回退。
          把 high_water 一起清掉的话,复位后的第 1 代会被判成合法新代际。
        ★ QUARANTINED / TERMINATING 是显式运维 tombstone:普通 Fleet 对账不能因同 UID、
          换 UID 或刷新 TTL 把它复活,恢复只能走受控 purge/recreate。
        """
        if instance_uid == "":
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "hub auth init requires instance uid"
            )
        key = L.auth_key(pod)
        px = int(auth_ttl_sec * 1000)
        for attempt in range(L.HUB_AUTH_CAS_RETRIES):
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(*L.fenced_watch_keys([key], pod, self._fence))
                    advance = await L.guard_writer_fence(pipe, pod, self._fence)
                    raw = await pipe.get(key)
                    rec = hubpb.HubShardAuthStorageRecord()
                    if raw is None:
                        rec.pod_name = pod
                        rec.instance_uid = instance_uid
                        rec.protocol_epoch = 1
                        rec.phase = hubpb.HUB_AUTH_PHASE_BOOTSTRAP
                        rec.required_writer_epoch = DS_AUTH_WRITER_EPOCH_V2
                    else:
                        rec.ParseFromString(bytes(raw))
                        # future writer 的权威记录只能由对应未来二进制处理。V2 不得在
                        # InitAuth 中复位实例、刷新 TTL 或覆盖 unknown fields。
                        if rec.required_writer_epoch != DS_AUTH_WRITER_EPOCH_V2 or not (
                            L.hub_stored_credential_epochs_v2(rec)
                        ):
                            raise AuthStaleError
                        if L.phase_locked(rec.phase):
                            raise AuthStaleError
                        if rec.instance_uid != instance_uid:
                            rec.pod_name = pod
                            rec.instance_uid = instance_uid
                            rec.protocol_epoch += 1
                            if rec.protocol_epoch > L._UINT32_MAX:  # noqa: SLF001
                                # Go 是 uint32 自增回绕成 0,紧接着的 `== 0 → 1` 兜底;
                                # Python 的 int 无限精度,必须显式回绕才不会写出一个
                                # proto 装不下的 epoch(SerializeToString 会直接报错)。
                                rec.protocol_epoch = 0
                            if rec.protocol_epoch == 0:
                                rec.protocol_epoch = 1
                            rec.phase = hubpb.HUB_AUTH_PHASE_BOOTSTRAP
                            rec.ClearField("active")
                            rec.ClearField("pending")
                            rec.pending_started_ms = 0
                            rec.delivered_rv = ""
                    rec.updated_at_ms = L.now_ms()
                    payload = rec.SerializeToString()
                    pipe.multi()
                    advance(pipe)
                    pipe.set(key, payload, px=px)
                    await pipe.execute()
                    return rec
            except WatchError:
                await L.cas_conflict_backoff(attempt)
        raise errcode.PandoraError(
            errcode.ErrUnavailable, "hub auth init %s: cas retry exhausted", pod
        )

    # ── StagePending ─────────────────────────────────────────────────────────

    async def stage_pending(self, pod: str, cred, auth_ttl_sec: float):
        """暂存 pending 凭据。Go: StagePending。

        ★ `gen > high_water_gen` **且** `gen > active.gen` 两条都要:前者杜绝计数器
          复位后的 gen 复用,后者杜绝把一个比当前 active 还旧的凭据 stage 进来。
        ★ 存的是**克隆**,不是调用方传进来的那个对象 —— 调用方在返回后继续改同一个
          proto 指针时,已落库的字节不会跟着变(Go 用 proto.Clone,同理)。
        """
        L.validate_stored_credential(cred, L.now_ms())
        key = L.auth_key(pod)
        px = int(auth_ttl_sec * 1000)
        for attempt in range(L.HUB_AUTH_CAS_RETRIES):
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(*L.fenced_watch_keys([key], pod, self._fence))
                    advance = await L.guard_writer_fence(pipe, pod, self._fence)
                    raw = await pipe.get(key)
                    if raw is None:
                        raise AuthStaleError  # 未 InitAuth
                    rec = hubpb.HubShardAuthStorageRecord()
                    rec.ParseFromString(bytes(raw))
                    if L.phase_locked(rec.phase):
                        raise AuthStaleError  # QUARANTINED/TERMINATING:相位锁定
                    if (
                        rec.instance_uid != cred.instance_uid
                        or rec.protocol_epoch != cred.protocol_epoch
                    ):
                        raise AuthStaleError  # 实例 / 纪元不符
                    if (
                        not L.hub_auth_record_v2_exact(rec)
                        or cred.writer_epoch != DS_AUTH_WRITER_EPOCH_V2
                    ):
                        raise AuthStaleError
                    if cred.gen <= rec.high_water_gen:
                        raise AuthStaleError  # gen 必须严格高于历史水位
                    if rec.HasField("active") and cred.gen <= rec.active.gen:
                        raise AuthStaleError
                    rec.pending.CopyFrom(cred)
                    rec.high_water_gen = cred.gen
                    rec.pending_started_ms = L.now_ms()
                    rec.delivered_rv = ""
                    rec.phase = (
                        hubpb.HUB_AUTH_PHASE_ROTATING
                        if rec.HasField("active")
                        else hubpb.HUB_AUTH_PHASE_BOOTSTRAP
                    )
                    rec.updated_at_ms = L.now_ms()
                    payload = rec.SerializeToString()
                    pipe.multi()
                    advance(pipe)
                    pipe.set(key, payload, px=px)
                    await pipe.execute()
                    return rec
            except WatchError:
                await L.cas_conflict_backoff(attempt)
        raise errcode.PandoraError(
            errcode.ErrUnavailable, "hub auth stage %s: cas retry exhausted", pod
        )

    # ── MarkDelivered ────────────────────────────────────────────────────────

    async def mark_delivered(self, pod: str, expected, rv: str, auth_ttl_sec: float) -> None:
        """记 expected pending 已 PATCH 投递到某 GameServer resourceVersion。Go: MarkDelivered。

        ★ 事务提交时 pending 必须**仍与 expected 完全一致**:旧 PATCH 响应晚到时,
          不能把 delivered_rv 写到一个更高代际的 pending 上(那会让投递侧以为新代际
          已经送达,于是停止重投)。
        """
        L.validate_stored_credential(expected, L.now_ms())
        if rv == "":
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "hub auth mark delivered requires resource version"
            )
        key = L.auth_key(pod)
        px = int(auth_ttl_sec * 1000)
        for attempt in range(L.HUB_AUTH_CAS_RETRIES):
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(*L.fenced_watch_keys([key], pod, self._fence))
                    advance = await L.guard_writer_fence(pipe, pod, self._fence)
                    raw = await pipe.get(key)
                    if raw is None:
                        raise AuthStaleError
                    rec = hubpb.HubShardAuthStorageRecord()
                    rec.ParseFromString(bytes(raw))
                    if (
                        L.phase_locked(rec.phase)
                        or not L.hub_auth_record_v2_exact(rec)
                        or expected.writer_epoch != DS_AUTH_WRITER_EPOCH_V2
                        or rec.instance_uid != expected.instance_uid
                        or rec.protocol_epoch != expected.protocol_epoch
                        or not L.stored_credential_equal(
                            rec.pending if rec.HasField("pending") else None, expected
                        )
                    ):
                        raise AuthStaleError
                    rec.delivered_rv = rv
                    rec.updated_at_ms = L.now_ms()
                    payload = rec.SerializeToString()
                    pipe.multi()
                    advance(pipe)
                    pipe.set(key, payload, px=px)
                    await pipe.execute()
                    return
            except WatchError:
                await L.cas_conflict_backoff(attempt)
        raise errcode.PandoraError(
            errcode.ErrUnavailable, "hub auth mark delivered %s: cas retry exhausted", pod
        )

    # ── ActivateHeartbeat(唯一线性化点)─────────────────────────────────────

    async def activate_heartbeat(  # noqa: C901 —— 八个编号步骤与 Go 逐条对应
        self, pod: str, ident: CredentialIdentity, inp: ActivateHeartbeatInput
    ) -> ActivateResult:
        """单事务(authKey + shardKey + 六把 ledger key 同 slot)完成:

            ① 授权记录存在 → ② 相位未锁定 → ③ 记录级 uid/epoch 匹配
            → ④ 匹配 pending(线性化点)/ active(幂等)/ 都不匹配(stale)
            → ⑤ 分片镜像存在?否则不 promote、不写键
            → ⑥ promote(若匹配 pending)→ ⑦ 应用心跳 + 投影 active 元组
            → ⑧ 一次 EXEC 写 auth + shard + ledger

        Go: ActivateHeartbeat。
        """
        if (
            ident.gen == 0
            or ident.jti == ""
            or ident.instance_uid == ""
            or ident.protocol_epoch == 0
            or ident.token_sha256 == ""
            or ident.kid == ""
            or ident.writer_epoch != DS_AUTH_WRITER_EPOCH_V2
        ):
            raise AuthStaleError
        if inp.player_count < 0 or inp.max_players == 0 or len(inp.player_ids) != inp.player_count:
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "hub heartbeat count/max_players invalid"
            )
        seen: set[int] = set()
        for player_id in inp.player_ids:
            if player_id == 0:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg, "hub heartbeat player_ids contains zero"
                )
            if player_id in seen:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg, "hub heartbeat player_ids contains duplicate"
                )
            seen.add(player_id)

        a_key, s_key = L.auth_key(pod), L.shard_key(pod)
        watch_keys = L.fenced_watch_keys(
            [a_key, s_key, *L.capacity_ledger_keys(pod)], pod, self._fence
        )
        # ★ 权威心跳时刻**只取服务端接收时间**。请求 ts_ms 仅可用于遥测:
        #   一个未来时间戳能让失联 DS 长期保持「心跳新鲜」从而持续可分配。
        server_now_ms = L.now_ms()
        for attempt in range(L.HUB_AUTH_CAS_RETRIES):
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(*watch_keys)
                    advance = await L.guard_writer_fence(pipe, pod, self._fence)
                    out = ActivateResult()

                    a_raw = await pipe.get(a_key)  # ①
                    if a_raw is None:
                        raise AuthStaleError
                    auth_rec = hubpb.HubShardAuthStorageRecord()
                    auth_rec.ParseFromString(bytes(a_raw))
                    if (
                        auth_rec.pod_name != pod
                        or not L.hub_auth_record_v2_exact(auth_rec)
                        or ident.writer_epoch != DS_AUTH_WRITER_EPOCH_V2
                    ):
                        raise AuthStaleError
                    if L.phase_locked(auth_rec.phase):  # ②
                        raise AuthStaleError
                    if (  # ③ 记录级身份:uid/epoch 必须匹配(gen 单独不安全)
                        auth_rec.instance_uid != ident.instance_uid
                        or auth_rec.protocol_epoch != ident.protocol_epoch
                    ):
                        raise AuthStaleError

                    # ④ pending(线性化点)/ active(幂等)/ 都不匹配(stale)
                    pending = auth_rec.pending if auth_rec.HasField("pending") else None
                    active = auth_rec.active if auth_rec.HasField("active") else None
                    if L.routable_credential_complete(
                        pending, auth_rec, server_now_ms
                    ) and L.cred_matches(pending, ident, server_now_ms):
                        promote = True
                    elif L.routable_credential_complete(
                        active, auth_rec, server_now_ms
                    ) and L.cred_matches(active, ident, server_now_ms):
                        promote = False  # 幂等:已是 active
                    else:
                        raise AuthStaleError

                    s_raw = await pipe.get(s_key)  # ⑤
                    if s_raw is None:
                        # 不 promote、不写键:交 biz reconcile 拓扑后重试,
                        # 保证 promote 与 warming→ready 恒同事务。
                        await L._validate_snapshot(pipe, watch_keys)  # noqa: SLF001
                        return out
                    shard = L.unmarshal_shard(pod, s_raw)
                    # ★ MaxPlayers 是 DS 运行时 GameSession 的真实值。必须在 promote、
                    #   心跳时间、ready/state、ledger cleanup 等**任何副作用之前**与
                    #   allocator capacity 精确相等 —— 不等就说明两边对「这台机器能装
                    #   几个人」的认知已经分叉,继续往下走会按错的上限发座位。
                    if shard.capacity <= 0 or inp.max_players != shard.capacity:
                        raise errcode.PandoraError(
                            errcode.ErrInvalidState,
                            "hub heartbeat max_players=%d does not match capacity=%d",
                            inp.max_players,
                            shard.capacity,
                        )

                    led = await L.load_hub_capacity_ledger(pipe, pod, shard.capacity)
                    # ★ Heartbeat 只清 reservation 绝对过期、旧格式 session 到期和 UID 漂移;
                    #   `player_ids` 缺席**绝不能**释放 connected ownership(那是把一台
                    #   可能正在网络分区里的 DS 的漏报,当成玩家已经离场)。
                    L.prune_ledger(
                        led,
                        pod=pod,
                        uid=auth_rec.instance_uid,
                        epoch=auth_rec.protocol_epoch,
                        writer=DS_AUTH_WRITER_EPOCH_V2,
                        at_ms=server_now_ms,
                    )
                    L.sync_shard_capacity_projection(shard, led)

                    if promote:  # ⑥
                        auth_rec.active.CopyFrom(auth_rec.pending)
                        auth_rec.ClearField("pending")
                        auth_rec.pending_started_ms = 0
                        auth_rec.delivered_rv = ""
                        auth_rec.phase = hubpb.HUB_AUTH_PHASE_ACTIVE
                        out.accepted = True
                    auth_rec.updated_at_ms = server_now_ms
                    auth_rec.last_active_heartbeat_ms = server_now_ms

                    # ⑦ 应用心跳到分片镜像 + 投影 active 元组(与 promote 同事务)
                    L.apply_heartbeat_state_to_shard(shard, inp.state, server_now_ms)
                    shard.reported_connected_count = inp.player_count
                    shard.reported_max_players = inp.max_players
                    shard.last_verified_gen = auth_rec.active.gen
                    shard.last_verified_jti = auth_rec.active.jti
                    shard.gameserver_uid = auth_rec.instance_uid
                    shard.auth_epoch = auth_rec.protocol_epoch
                    shard.last_verified_writer_epoch = auth_rec.active.writer_epoch

                    auth_payload = auth_rec.SerializeToString()
                    shard_payload = L.marshal_shard(shard)
                    pipe.multi()  # ⑧
                    advance(pipe)
                    L.write_hub_capacity_ledger(pipe, pod, led)
                    pipe.set(a_key, auth_payload, px=int(inp.auth_ttl_sec * 1000))
                    pipe.set(s_key, shard_payload, px=int(inp.shard_ttl_sec * 1000))
                    await pipe.execute()

                    out.shard_found = True
                    out.shard_state = shard.state
                    out.active_gen = auth_rec.active.gen
                    out.active_jti = auth_rec.active.jti
                    out.instance_uid = auth_rec.instance_uid
                    out.protocol_epoch = auth_rec.protocol_epoch
                    out.writer_epoch = auth_rec.active.writer_epoch

                # 全局索引与 {pod} **不同 slot**,只能在事务外独立发;心跳高频,
                # 失败下次即补,所以刻意吞掉错误而不是让整次心跳失败。
                try:
                    await self._rdb.sadd(L.SHARDS_SET_KEY, pod)
                    await self._rdb.zadd(L.ACTIVE_KEY, {pod: float(server_now_ms)})
                except Exception:  # noqa: BLE001,S110 —— 幂等索引,下次心跳自然补
                    pass
                return out
            except WatchError:
                await L.cas_conflict_backoff(attempt)
        raise errcode.PandoraError(
            errcode.ErrUnavailable, "hub activate heartbeat %s: cas retry exhausted", pod
        )

    # ── QuarantineExpected ───────────────────────────────────────────────────

    async def quarantine_expected(
        self, pod: str, expected: CredentialIdentity, auth_ttl_sec: float, shard_ttl_sec: float
    ) -> QuarantineResult:
        """泄露凭据的紧急 fail-closed 吊销。Go: QuarantineExpected。

        ★ **不接受「按 pod 名盲吊销」**:调用方必须提交当前完整 active 身份,
          否则一个迟到的运维请求会误隔离同名重建后的**新** GameServer。
        ★ tombstone 写成**持久键**(TTL=0):有限 authTTL 会在 allocator 停机后自动
          丢失,于是仍存活的同 UID GameServer 可以被重新 Init/Stage —— 吊销失效。
        """
        if (
            pod == ""
            or expected.instance_uid == ""
            or expected.protocol_epoch == 0
            or expected.gen == 0
            or expected.jti == ""
            or expected.kid == ""
            or expected.token_sha256 == ""
            or expected.writer_epoch != DS_AUTH_WRITER_EPOCH_V2
            or auth_ttl_sec <= 0
            or shard_ttl_sec <= 0
        ):
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "hub quarantine requires full expected credential and ttls"
            )
        a_key, s_key = L.auth_key(pod), L.shard_key(pod)
        watch_keys = L.fenced_watch_keys([a_key, s_key], pod, self._fence)
        for attempt in range(L.HUB_AUTH_CAS_RETRIES):
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(*watch_keys)
                    advance = await L.guard_writer_fence(pipe, pod, self._fence)
                    result = QuarantineResult()
                    auth_raw = await pipe.get(a_key)
                    if auth_raw is None:
                        await L._validate_snapshot(pipe, watch_keys)  # noqa: SLF001
                        return result
                    auth_rec = hubpb.HubShardAuthStorageRecord()
                    auth_rec.ParseFromString(bytes(auth_raw))
                    active = auth_rec.active if auth_rec.HasField("active") else None
                    if (
                        auth_rec.pod_name != pod
                        or not L.hub_auth_record_v2_exact(auth_rec)
                        or expected.writer_epoch != DS_AUTH_WRITER_EPOCH_V2
                        or auth_rec.instance_uid != expected.instance_uid
                        or auth_rec.protocol_epoch != expected.protocol_epoch
                        or active is None
                        or not L.cred_matches(active, expected)
                        or auth_rec.phase
                        not in (
                            hubpb.HUB_AUTH_PHASE_ACTIVE,
                            hubpb.HUB_AUTH_PHASE_ROTATING,
                            hubpb.HUB_AUTH_PHASE_QUARANTINED,
                        )
                    ):
                        await L._validate_snapshot(pipe, watch_keys)  # noqa: SLF001
                        return result

                    shard = None
                    shard_raw = await pipe.get(s_key)
                    if shard_raw is not None:
                        shard = L.unmarshal_shard(pod, shard_raw)

                    auth_rec.phase = hubpb.HUB_AUTH_PHASE_QUARANTINED
                    auth_rec.ClearField("pending")
                    auth_rec.pending_started_ms = 0
                    auth_rec.delivered_rv = ""
                    auth_rec.updated_at_ms = L.now_ms()
                    auth_payload = auth_rec.SerializeToString()

                    shard_payload = None
                    projection_matches = _projection_matches_credential(auth_rec, shard, expected)
                    if projection_matches:
                        if shard.state != "stopping":
                            shard.state = "draining"
                        if shard.draining_since_ms == 0:
                            shard.draining_since_ms = auth_rec.updated_at_ms
                        shard_payload = L.marshal_shard(shard)

                    pipe.multi()
                    advance(pipe)
                    pipe.set(a_key, auth_payload)  # ★ 持久 tombstone,刻意不带 TTL
                    if shard_payload is not None:
                        pipe.set(s_key, shard_payload, px=int(shard_ttl_sec * 1000))
                    await pipe.execute()
                    result.auth_quarantined = True
                    result.projection_drained = projection_matches
                    return result
            except WatchError:
                await L.cas_conflict_backoff(attempt)
        raise errcode.PandoraError(
            errcode.ErrUnavailable, "hub quarantine %s: cas retry exhausted", pod
        )

    # ── 路由检查 ─────────────────────────────────────────────────────────────

    async def reserve_routable_seat(
        self, pod: str, at_ms: int, max_heartbeat_age_ms: int, shard_ttl_sec: float
    ) -> L.ReserveResult:
        """**已停用**的整数 seat++ 路径。Go: ReserveRoutableSeat(同样只返回错误)。

        ★ 保留这个方法而不是删掉,是因为它是一块**墓碑**:整数座位与逐 assignment
          reservation 并存时,同一个玩家会占两个座位、且退座路径不对称。
          留着让误用当场炸,比让调用方找不到方法而自己再写一个整数计数器安全。
        """
        raise errcode.PandoraError(
            errcode.ErrInvalidState, "integer hub seat reservation is disabled"
        )

    async def check_routable(
        self, pod: str, at_ms: int, max_heartbeat_age_ms: int
    ) -> L.ReserveResult:
        """只读可路由检查(不占座、不写键)。Go: CheckRoutable → routable(reserve=false)。

        幂等重签 / 复用已有归属时用它校验目标分片当前是否可路由,并取回当前 active
        元组,供比对归属记录钉的元组是否仍等于当前 active(实例漂移即失效)。
        """
        if at_ms <= 0:
            at_ms = L.now_ms()
        a_key, s_key = L.auth_key(pod), L.shard_key(pod)
        watch_keys = L.fenced_watch_keys([a_key, s_key], pod, self._fence)
        for attempt in range(L.HUB_AUTH_CAS_RETRIES):
            try:
                async with self._rdb.pipeline(transaction=True) as pipe:
                    await pipe.watch(*watch_keys)
                    # ★ 只读路径**不**做 fence 校验、也不推进水位:失主副本的纯读路由
                    #   判断不该被误拒(写入仍由各写事务自身的 fence 拦截)。
                    out = await _routable_snapshot(pipe, pod, at_ms, max_heartbeat_age_ms)
                    # 只 WATCH 不 EXEC 不具备快照一致性 —— 必须跑一次只读事务。
                    pipe.multi()
                    pipe.get(a_key)
                    pipe.get(s_key)
                    await pipe.execute()
                    return out
            except WatchError:
                await L.cas_conflict_backoff(attempt)
        raise errcode.PandoraError(
            errcode.ErrUnavailable, "hub reserve routable %s: cas retry exhausted", pod
        )


# ── 事务内的纯函数 ───────────────────────────────────────────────────────────


async def _routable_snapshot(
    pipe: Any, pod: str, at_ms: int, max_heartbeat_age_ms: int
) -> L.ReserveResult:
    """Go: routable() 的读取 + 判据部分(reserve=false 分支)。

    ★ 判据顺序与 Go 逐条一致,包括那些看起来与 `modelb_routable_reason` 重复的:
      Go 在调用 modelBRoutableReason 之后又单独判了 state/track/gen/jti/uid/epoch/
      heartbeat/writer_epoch —— 重复不是冗余,而是**reason 字符串不同**
      (`shard-instance-mismatch` / `shard-writer-epoch-mismatch` /
      `shard-release-track-invalid` 只有这里才产出)。运维就是照着这些字符串定位的。
    """
    from pandorapy import releasetrack

    out = L.ReserveResult()
    a_raw = await pipe.get(L.auth_key(pod))
    if a_raw is None:
        out.reason = "auth-missing"
        return out
    auth_rec = hubpb.HubShardAuthStorageRecord()
    auth_rec.ParseFromString(bytes(a_raw))
    if auth_rec.pod_name != pod:
        out.reason = "auth-pod-mismatch"
        return out
    if not L.phase_serving(auth_rec.phase):
        out.reason = "phase-not-active"
        return out
    active = auth_rec.active if auth_rec.HasField("active") else None
    if not L.routable_credential_complete(active, auth_rec, at_ms):
        out.reason = "no-active"
        return out

    s_raw = await pipe.get(L.shard_key(pod))
    if s_raw is None:
        out.reason = "shard-missing"
        return out
    shard = L.unmarshal_shard(pod, s_raw)
    reason = L.modelb_routable_reason(auth_rec, shard, pod, at_ms, max_heartbeat_age_ms, None)
    if reason:
        out.reason = reason
        return out
    if shard.state != "ready":
        out.reason = "shard-not-ready"
        return out
    release_track = shard.release_track or releasetrack.STABLE  # 旧记录的 additive 迁移
    if not releasetrack.valid(release_track):
        out.reason = "shard-release-track-invalid"
        return out
    # active == shard.last_verified:确保分片镜像正是被当前 active 凭据投影的那份。
    if shard.last_verified_gen != active.gen or shard.last_verified_jti != active.jti:
        out.reason = "shard-not-verified-by-active"
        return out
    if shard.gameserver_uid != auth_rec.instance_uid or shard.auth_epoch != auth_rec.protocol_epoch:
        out.reason = "shard-instance-mismatch"
        return out
    # ★ 心跳新鲜度只认 auth 记录里由 Model B 原子激活路径写入的**服务端**时间:
    #   legacy writer 只能改 shard.last_heartbeat_ms,无法借此越过 activation fence。
    if auth_rec.last_active_heartbeat_ms <= 0 or auth_rec.last_active_heartbeat_ms > at_ms:
        out.reason = "heartbeat-invalid"
        return out
    if shard.last_verified_writer_epoch != active.writer_epoch:
        out.reason = "shard-writer-epoch-mismatch"
        return out
    if (
        max_heartbeat_age_ms > 0
        and at_ms - auth_rec.last_active_heartbeat_ms > max_heartbeat_age_ms
    ):
        out.reason = "heartbeat-stale"
        return out

    out.active_gen = active.gen
    out.active_jti = active.jti
    out.instance_uid = auth_rec.instance_uid
    out.protocol_epoch = auth_rec.protocol_epoch
    out.writer_epoch = active.writer_epoch
    out.shard_id = shard.shard_id
    out.hub_addr = shard.hub_addr
    out.region = shard.region
    out.capacity = shard.capacity
    out.release_track = release_track
    out.player_count = shard.player_count
    out.ok = True
    return out


def _projection_matches_credential(auth_rec, shard, expected: CredentialIdentity) -> bool:
    """Go: hubProjectionMatchesCredential。

    授权记录、分片镜像、expected 三者必须描述**同一份** active 凭据 —— 少一条比较
    就可能把一个正在轮换中的分片按旧凭据 drain 掉。
    """
    if auth_rec is None or shard is None or not auth_rec.HasField("active"):
        return False
    active = auth_rec.active
    return (
        L.hub_auth_record_v2_exact(auth_rec)
        and active.writer_epoch == DS_AUTH_WRITER_EPOCH_V2
        and auth_rec.pod_name == shard.hub_pod_name
        and auth_rec.instance_uid == expected.instance_uid
        and auth_rec.protocol_epoch == expected.protocol_epoch
        and shard.gameserver_uid == expected.instance_uid
        and shard.auth_epoch == expected.protocol_epoch
        and active.gen == expected.gen
        and active.jti == expected.jti
        and active.writer_epoch == expected.writer_epoch
        and shard.last_verified_gen == expected.gen
        and shard.last_verified_jti == expected.jti
        and shard.last_verified_writer_epoch == expected.writer_epoch
    )
