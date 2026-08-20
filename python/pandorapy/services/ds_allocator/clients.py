"""ds_allocator 的两个下游 gRPC 客户端 —— 忠实移植 Go 侧:

    services/battle/ds_allocator/internal/data/owner_lease_client.go  (213 行)
    services/battle/ds_allocator/internal/data/locator_client.go      ( 79 行)

两者共性:**内网 insecure 直连、不带玩家 JWT**(与 Go 的
`grpcclient.MustDialInsecure` 一致)。owner 服务对带玩家 JWT 的调用是直接拒的
——它认的是内网系统调用,所以这里不能顺手复用带鉴权的 channel。

────────────────────────────────────────────────────────────────────────────────
★ 这一层只做**搬运**,不做任何归属 / 离场判定

  归属判定全在 owner 服务的线性一致事务里(§9.22);presence 判定在 locator。
  本模块唯一的职责是把 View ↔ proto 翻过来翻过去,并保证:

    ① 每一次 unary 调用都有**有界超时**(§9 不变量 19/20);
    ② 失败一律**上抛**,绝不返回一个"看起来正常"的默认值;
    ③ Go 侧"出错也要把值带回去"的两处多返回,用 `PandoraError` 的
       **声明式 slots** 携带,而不是 `setattr`。

★ locator 失败**不等于**玩家已离场(§9.22 / 需求逐条第 4 条)

  `refresh_battle_locations` 写的是 presence 投影(BATTLE + match_id +
  battle_pod)。它写失败只说明"投影没刷新",**不能**被上层解释成:
    - 玩家不在这局了;
    - 玩家已经离开旧 DS;
    - 可以让另一台 DS 接管他。
  真正的离场证明只有 `departure.py` 里那两种(census 缺席 / teardown proof)。
  所以本函数的失败一律抛异常交给 biz 记 Warn(Go 是返回 firstErr,biz 只 Warn
  不阻断心跳/对局),**绝不**把失败吞成"刷新成功"。
"""

from __future__ import annotations

import dataclasses

import grpc
from pandora.common.v1 import errcode_pb2
from pandora.locator.v1 import locator_pb2, locator_pb2_grpc
from pandora.owner.v1 import owner_pb2 as ownerpb
from pandora.owner.v1 import owner_pb2_grpc as owner_grpc

from pandorapy import errcode, placement

# Go: `ownerLeaseRPCTimeout = 2 * time.Second`。
#
# ★ 单次续租调用上限。它挂在 **DS 心跳响应返回之前**,必须远小于 DS 心跳周期(5s):
#   取大了会让"为了保住租约"的同步调用把心跳本身拖过 heartbeat_timeout,于是
#   续租机制把自己搞死 —— DS 反而因为"心跳超时"被判掉线并被补偿回收。
OWNER_LEASE_RPC_TIMEOUT_SEC = 2.0

# ★ Go 的 `locator_client.go` **没有** per-call 超时(直接把 biz 传进来的 ctx
#   交给 SetLocation)。这里刻意补一个 —— 见模块尾部「与 Go 的已知差异」①。
#   取 3.0s:与 `hub_allocator/locator_client.py` 的 `DEFAULT_TIMEOUT_SEC` 同值,
#   两个 locator 客户端的等待上界不该一个 3 秒一个无限。
LOCATOR_RPC_TIMEOUT_SEC = 3.0

_UINT32_MAX = (1 << 32) - 1
_UINT64_MAX = (1 << 64) - 1


def _check_uint(name: str, value: int, limit: int) -> int:
    """发送前的无符号范围校验(Go 由 uint32/uint64 类型系统免费获得)。

    ★ 不校验的话越界值会在 protobuf 序列化时抛 `ValueError` —— 一个既不带业务码、
      也说不清是哪个字段的异常,而且是在**已经发起调用之后**才炸。
    """
    if not isinstance(value, int) or isinstance(value, bool):
        raise errcode.PandoraError(
            errcode.ErrInvalidArg, "owner %s must be int, got %r", name, value
        )
    if value < 0 or value > limit:
        raise errcode.PandoraError(errcode.ErrInvalidArg, "owner %s out of range: %d", name, value)
    return value


# ── View(pb 解耦:biz 依赖本类型,不依赖生成代码)──────────────────────────


@dataclasses.dataclass(frozen=True, slots=True)
class OwnerTargetView:
    """exact DS 实例身份视图。Go: `data.OwnerTargetView`。

    ★ frozen + slots 让 `==` 变成**逐字段**比较 —— "任何一格变了都算归属变了",
      这正是上层对账 `current != target` 依赖的语义。
    """

    pod_name: str = ""
    instance_uid: str = ""
    instance_epoch: int = 0
    assignment_or_allocation_id: str = ""
    release_track: str = ""
    # 本次归属的来源版本(INC-20260818-003)。
    #
    # ★ 刻意**不进 proto 的 OwnerTarget**:OwnerTarget 是 exact 身份,Admit 要拿它
    #   做全等校验,把一个会变的版本号塞进身份里会让"同一归属的重复 Admit"变成
    #   identity mismatch。它只随 BeginTransitionRequest 单独发出去。
    #
    #   0 = 本部署未启用写者租约(dev / 单副本),或调用方尚未滚上本协议。
    source_revision: int = 0


@dataclasses.dataclass(frozen=True, slots=True)
class OwnerRecordView:
    """当前 owner 记录视图(migrate 决策用最小字段集)。Go: `data.OwnerRecordView`。"""

    owner_epoch: int = 0
    owner_type: int = 0
    phase: int = 0
    pod_name: str = ""
    instance_uid: str = ""
    instance_epoch: int = 0
    assignment_or_allocation_id: str = ""
    release_track: str = ""
    operation_id: str = ""
    admit_not_before_ms: int = 0


def target_proto(t: OwnerTargetView) -> ownerpb.OwnerTarget:
    """View → proto。★ 不含 source_revision(见 `OwnerTargetView.source_revision`)。"""
    return ownerpb.OwnerTarget(
        pod_name=t.pod_name,
        instance_uid=t.instance_uid,
        instance_epoch=_check_uint("instance_epoch", t.instance_epoch, _UINT32_MAX),
        assignment_or_allocation_id=t.assignment_or_allocation_id,
        release_track=t.release_track,
    )


def record_view(r) -> OwnerRecordView:  # noqa: ANN001
    """proto → View。对齐 Go `recordView` 的 `GetX()` nil-safe 语义。

    ★ Go 侧 `OwnerType` / `Phase` 是 `int8`,Python 用 `int`。这不是放宽:
      两者都只是**搬运**枚举数值,判定在 owner 服务侧;而 int8 在 Python 里
      没有对应类型,硬造一个只会在越界时多一处不必要的失败点。
    """
    if r is None:
        return OwnerRecordView()
    return OwnerRecordView(
        owner_epoch=int(r.owner_epoch),
        owner_type=int(r.owner_type),
        phase=int(r.phase),
        pod_name=r.target.pod_name,
        instance_uid=r.target.instance_uid,
        instance_epoch=int(r.target.instance_epoch),
        assignment_or_allocation_id=r.target.assignment_or_allocation_id,
        release_track=r.target.release_track,
        operation_id=r.operation_id,
        admit_not_before_ms=int(r.admit_not_before_ms),
    )


# ── owner 权威客户端 ────────────────────────────────────────────────────────


class GrpcOwnerLeaseRenewer:
    """owner 服务 gRPC 客户端。Go: `data.GrpcOwnerLeaseRenewer`。

    ## Go 的 `(value, err)` 在 Python 怎么表达

    Go 侧有两处刻意"**出错也要把值带回去**":

      - `BeginTransition` 非 OK 时仍返回 `recordView(resp.GetRecord())` ——
        EPOCH_CONFLICT 时那是**权威当前记录**,调用方要靠它决定重查 / 回滚
        (Release 要求 owner_epoch + operation_id 全等,自铸或推算都不行);
      - `Admit` 非 OK 时仍返回 `resp.GetRetryAfterMs()` —— >0 表示"再入屏障还没开",
        是 §9.23 预期中的 **WAIT**,不能计进失败告警。

    Python 不能一边 raise 一边 return,所以把这两个值挂在异常的
    `current_record` / `retry_after_ms` **声明槽位**上(`errcode.PandoraError`
    的 `__slots__`,不是 `setattr`)。

    ★ 不要改成"失败返回 None 不抛" —— 那样调用方少写一个 if 就把归属写失败
      当成写成功了,而这条路径的失败必须 fail-closed(§9.22)。
    """

    __slots__ = ("_channel", "_stub", "_timeout_sec", "_owns_channel")

    def __init__(
        self,
        owner_addr: str = "",
        *,
        channel: grpc.aio.Channel | None = None,
        timeout_sec: float = OWNER_LEASE_RPC_TIMEOUT_SEC,
    ) -> None:
        if channel is not None:
            # 注入的 channel 由调用方持有,close() 不碰它 —— 关掉别人的 channel
            # 会让共享它的其它 client 在下一次调用时莫名其妙地拿到 CANCELLED。
            self._channel = channel
            self._owns_channel = False
        else:
            if not owner_addr:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg, "owner addr required to build owner lease renewer"
                )
            self._channel = grpc.aio.insecure_channel(owner_addr)
            self._owns_channel = True
        self._stub = owner_grpc.OwnerServiceStub(self._channel)
        self._timeout_sec = timeout_sec

    async def close(self) -> None:
        """Go: `Close`(只关自己拨的连接)。"""
        if self._owns_channel:
            await self._channel.close()

    async def renew_instance_lease(
        self, pod_name: str, instance_uid: str, instance_epoch: int, release_track: str
    ) -> None:
        """续写实例租约(deadline 只前进,owner 侧幂等)。Go: `RenewInstanceLease`。"""
        resp = await self._stub.RenewInstanceLease(
            ownerpb.RenewInstanceLeaseRequest(
                target=ownerpb.OwnerTarget(
                    pod_name=pod_name,
                    instance_uid=instance_uid,
                    instance_epoch=_check_uint("instance_epoch", instance_epoch, _UINT32_MAX),
                    release_track=release_track,
                ),
                # 租约长度由**本侧**声明、owner 侧再钳一次(双保险):两边同源于
                # placement,谁也不能单方面把 DS 自我 fencing 的窗口拉长。
                lease_seconds=placement.DS_FENCE_LEASE_MAX_SECONDS,
            ),
            timeout=self._timeout_sec,
        )
        if resp.code != errcode_pb2.OK:
            raise errcode.PandoraError(
                int(resp.code), "owner renew lease rejected pod=%s uid=%s", pod_name, instance_uid
            )

    async def query_owner(self, player_id: int) -> OwnerRecordView:
        """读当前 owner 记录(query-first)。Go: `QueryOwner`。

        ★ 失败必须上抛:把"查不到"当成"没有归属"就是 §9.22 明令禁止的
          "冒充 OFFLINE",会直接导致第二个 DS 被放进来。
        """
        resp = await self._stub.QueryOwner(
            ownerpb.QueryOwnerRequest(player_id=_check_uint("player_id", player_id, _UINT64_MAX)),
            timeout=self._timeout_sec,
        )
        if resp.code != errcode_pb2.OK:
            raise errcode.PandoraError(int(resp.code), "owner query rejected player=%d", player_id)
        return record_view(resp.record)

    async def begin_transition(
        self,
        player_id: int,
        expect_epoch: int,
        operation_id: str,
        owner_type: int,
        target: OwnerTargetView,
    ) -> OwnerRecordView:
        """发起 owner 迁移(推进到 PENDING)。Go: `BeginTransition`。

        成功返回**新记录**(biz 要拿新记录的 owner_epoch + operation_id 才能在
        部分失败时精确 Release 回滚);非 OK 时把**权威当前记录**挂在异常的
        `current_record` 上供重查决策。
        """
        resp = await self._stub.BeginTransition(
            ownerpb.BeginTransitionRequest(
                player_id=_check_uint("player_id", player_id, _UINT64_MAX),
                expect_epoch=_check_uint("expect_epoch", expect_epoch, _UINT64_MAX),
                operation_id=operation_id,
                owner_type=owner_type,
                target=target_proto(target),
                # 来源版本单独发,不进 Target(见 OwnerTargetView.source_revision)。
                source_revision=_check_uint(
                    "source_revision", target.source_revision, _UINT64_MAX
                ),
            ),
            timeout=self._timeout_sec,
        )
        view = record_view(resp.record)
        if resp.code != errcode_pb2.OK:
            err = errcode.PandoraError(int(resp.code), "owner begin rejected player=%d", player_id)
            err.current_record = view
            raise err
        return view

    async def admit(
        self, player_id: int, owner_epoch: int, operation_id: str, target: OwnerTargetView
    ) -> int:
        """再入屏障后的 exact CAS(PENDING → ADMITTED)。成功返回 0。Go: `Admit`。

        非 OK 时把 `retry_after_ms` 挂在异常上:>0 表示**屏障还没到点**(旧 DS 的
        租约还没过安全截止),那是预期中的 WAIT,调用方应静默退避并在下次心跳重试,
        绝不能计进"权威写失败"的告警 —— 否则每次正常迁移都会刷一片假告警。
        """
        resp = await self._stub.Admit(
            ownerpb.AdmitRequest(
                player_id=_check_uint("player_id", player_id, _UINT64_MAX),
                owner_epoch=_check_uint("owner_epoch", owner_epoch, _UINT64_MAX),
                operation_id=operation_id,
                target=target_proto(target),
            ),
            timeout=self._timeout_sec,
        )
        if resp.code != errcode_pb2.OK:
            err = errcode.PandoraError(int(resp.code), "owner admit rejected player=%d", player_id)
            err.retry_after_ms = int(resp.retry_after_ms)
            raise err
        return 0

    async def release_owner(self, player_id: int, owner_epoch: int, operation_id: str) -> None:
        """释放 owner 记录(INC-20260729-002 P0-B1)。Go: `ReleaseOwner`。

        ★ 只在「被判弃实例的 GameServer 已确认回收」之后调用,且必须带上**从
          Query 读到的** owner_epoch + operation_id —— owner 侧按 compare-delete
          处理,epoch 不匹配即拒绝 / no-op(§9.23「迟到 Logout 只能 compare-delete
          自己,不能删除新会话」同款约束)。这样即使玩家在我们判弃期间已被迁到新
          DS(epoch 已 +1),本次释放也不会误删新归属。
        """
        resp = await self._stub.ReleaseOwner(
            ownerpb.ReleaseOwnerRequest(
                player_id=_check_uint("player_id", player_id, _UINT64_MAX),
                owner_epoch=_check_uint("owner_epoch", owner_epoch, _UINT64_MAX),
                operation_id=operation_id,
            ),
            timeout=self._timeout_sec,
        )
        if resp.code != errcode_pb2.OK:
            raise errcode.PandoraError(
                int(resp.code), "owner release rejected player=%d", player_id
            )


# ── locator 客户端 ──────────────────────────────────────────────────────────


class GrpcLocationRefresher:
    """player_locator gRPC 客户端。Go: `data.GrpcLocationRefresher`。

    心跳成功且对局 ready/running 时,把该对局玩家的位置刷新为 BATTLE(顺带续期
    locator TTL),使玩家整局在线期间都能被 login 检测到"在战斗中"。

    ## 状态权属(§9 不变量 1 / §9.22)

    BATTLE 态由 matchmaker 成局时**首次写入**;ds_allocator 心跳只做"同 match_id
    续期"(BATTLE→BATTLE 同 match),被 locator guard 放行。不同 match_id 的迟到
    心跳(旧 DS / 旧 allocator)会被 locator guard 拒,避免覆盖当前对局位置。

    ★ 本类写的是 **presence 投影**,不是归属权威。写成功不授权任何人接管玩家,
      写失败也不证明玩家离开了 —— 离场证明只在 `departure.py`。
    """

    __slots__ = ("_channel", "_stub", "_timeout_sec", "_owns_channel")

    def __init__(
        self,
        locator_addr: str = "",
        *,
        channel: grpc.aio.Channel | None = None,
        timeout_sec: float = LOCATOR_RPC_TIMEOUT_SEC,
    ) -> None:
        if channel is not None:
            # Go 的构造函数只接受现成 conn(main.go 负责 defer conn.Close());
            # 这里两种都支持,但注入的 channel 同样**不由本对象关闭**。
            self._channel = channel
            self._owns_channel = False
        else:
            if not locator_addr:
                raise errcode.PandoraError(
                    errcode.ErrInvalidArg, "locator addr required to build location refresher"
                )
            self._channel = grpc.aio.insecure_channel(locator_addr)
            self._owns_channel = True
        self._stub = locator_pb2_grpc.PlayerLocatorServiceStub(self._channel)
        self._timeout_sec = timeout_sec

    async def close(self) -> None:
        """Go: `Close`(只关自己拨的连接)。"""
        if self._owns_channel:
            await self._channel.close()

    async def refresh_battle_locations(
        self, player_ids: list[int], match_id: int, ds_addr: str
    ) -> None:
        """把这批玩家位置刷新为 BATTLE。Go: `RefreshBattleLocations`。

        逐玩家 best-effort:**单个失败继续其余**,最后抛出第一个错误供 biz 记 Warn。

        ★ "继续其余"不是容错洁癖:一次心跳带的是整局玩家,因为第 3 个玩家的
          locator 写失败就跳过第 4~10 个,会让那几个玩家的 presence TTL 到期,
          login 侧就看不到他们"在战斗中" —— 而他们其实好好地在打。

        ★ `match_id == 0 or ds_addr == ""` 直接 no-op(与 Go 一致):没有这两个值
          就构造不出一条**可被 locator guard 校验**的投影,写进去只会变成一条
          guard 拒绝的噪声,或者更糟 —— 一条 match_id 为 0 的投影盖掉真实位置。
        """
        if match_id == 0 or ds_addr == "":
            return
        _check_uint("match_id", match_id, _UINT64_MAX)
        first_err: BaseException | None = None
        for pid in player_ids:
            if pid == 0:
                continue
            _check_uint("player_id", pid, _UINT64_MAX)
            try:
                resp = await self._stub.SetLocation(
                    locator_pb2.SetLocationRequest(
                        player_id=pid,
                        location=locator_pb2.Location(
                            state=locator_pb2.LOCATION_STATE_BATTLE,
                            match_id=match_id,
                            battle_pod=ds_addr,
                        ),
                    ),
                    timeout=self._timeout_sec,
                )
            except grpc.aio.AioRpcError as exc:
                # Go: `errcode.New(errcode.ErrInternal, "locator SetLocation rpc: %v", err)`。
                # 消息逐字对齐,两栈日志能被同一条 Loki 查询命中。
                if first_err is None:
                    first_err = errcode.PandoraError(
                        errcode.ErrInternal,
                        "locator SetLocation rpc: %s",
                        exc.details() or exc.code().name,
                        cause=exc,
                    )
                continue
            if resp.code != errcode_pb2.OK and first_err is None:
                first_err = errcode.PandoraError(
                    int(resp.code), "locator SetLocation code=%d", int(resp.code)
                )
        if first_err is not None:
            raise first_err


# ────────────────────────────────────────────────────────────────────────────
# 与 Go 的已知差异(交付报告同步列出)
#
# ① **locator 调用补了 per-call 超时(Go 没有)。**
#    Go 的 `RefreshBattleLocations` 把 biz 传进来的 ctx 直接交给 SetLocation,
#    没有 `context.WithTimeout`。Go 侧靠 gRPC server 的整体 deadline 兜底;
#    Python 侧不补的话,locator 卡住会让整条心跳处理路径挂在一个**没有 deadline
#    的等待**里 —— §9 不变量 19/20 明令禁止,也是本次移植的硬性要求第 13 条。
#    这是**收紧**而非放宽:超时只会让本次刷新失败(biz 记 Warn 后继续),
#    不会改变任何权威判定。
#
# ② **传输层异常的表达。**
#    Go 直接把 grpc err 包成 `errcode.New(ErrInternal, "locator SetLocation rpc: %v", err)`;
#    Python 里 `%v` 对应 `exc.details() or exc.code().name`,并把原异常挂在 `cause`
#    上(`errcode.as_code` 会沿 `__cause__` 回溯,与 Go 的 `errors.As` 同语义)。
#    owner 客户端则与 Go 一致**原样上抛** `AioRpcError`(两侧都归到 ErrUnknown)。
#
# ③ **uint32/uint64 边界显式校验。**
#    Go 由类型系统免费获得;Python 不判会在 protobuf 序列化时抛一个不带业务码的
#    `ValueError`。这里在**发送前**判并抛 `ErrInvalidArg`。
#
# ④ **`OwnerRecordView.owner_type` / `phase` 用 `int` 而非 Go 的 `int8`。**
#    只是搬运枚举数值,判定在 owner 服务;Python 没有 int8 类型。
