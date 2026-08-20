"""owner 权威用例层 —— 对应 Go 侧 internal/biz/owner.go。

职责边界很窄且刻意如此:**入参形状校验** → 委托 OwnerRepo。
CAS / 屏障 / 幂等**全部在数据层的同一个事务内** —— 放到这一层做等于又造一个 TOCTOU。

fence 常量单一来源 pandorapy.placement:
  - skew margin 是 admit_not_before 的余量项
  - lease 秒数钳制到 DS_FENCE_LEASE_MAX_SECONDS
"""

from __future__ import annotations

from pandorapy import errcode, placement
from pandorapy import log as plog
from pandorapy.services.owner import data as odata

# 入参形状拒绝 reason 枚举(§11.3 R2)。
#
# ★ 为什么必须自己打日志:这些错误码(ErrInvalidArg / ErrOwnerInvalidOperation)
# 都不属 IsServerFault,access log 只记 DEBUG —— 不在业务侧打,线上完全看不到
# 谁在用垃圾参数敲 owner 权威,现象只剩下「这玩家归属一直推不动」。
REASON_PLAYER_ID_REQUIRED = "player_id_required"
REASON_OWNER_EPOCH_REQUIRED = "owner_epoch_required"
REASON_OPERATION_ID_INVALID = "operation_id_not_uuid_v4"
REASON_OWNER_TYPE_INVALID = "owner_type_invalid"
REASON_TARGET_INCOMPLETE = "target_identity_incomplete"
REASON_INSTANCE_INCOMPLETE = "instance_identity_incomplete"
REASON_LEASE_SECONDS_MISSING = "lease_seconds_required"


def _log_rejected(rpc: str, reason: str, player_id: int, **extra) -> None:
    """记录一次 owner 用例层的**副作用前**拒绝。

    ⚠️ 内部调用面不会自动注入 player_id,必须手写(与 Go 侧同一注意事项)。
    """
    plog.get().warning(
        "owner_request_rejected", rpc=rpc, reason=reason, player_id=player_id, **extra
    )


class OwnerUsecase:
    """owner 权威用例。对应 Go 的 biz.OwnerUsecase。"""

    __slots__ = ("_repo", "_cfg")

    def __init__(self, repo, cfg) -> None:
        self._repo = repo
        self._cfg = cfg

    async def query(self, player_id: int) -> odata.OwnerRecord:
        """读当前 owner 记录。

        ★ 调用方**查询失败一律按 UNKNOWN 处理**(§9.22),不得冒充 OFFLINE / 空闲。
        本层只保证:参数非法时明确拒绝,而不是返回一条空记录让调用方误判"没有 owner"。
        """
        if player_id == 0:
            _log_rejected("Query", REASON_PLAYER_ID_REQUIRED, player_id)
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        return await self._repo.query(player_id)

    async def begin_transition(
        self,
        player_id: int,
        expect_epoch: int,
        operation_id: str,
        owner_type: int,
        target: odata.OwnerTarget,
        source_revision: int = 0,
    ) -> odata.OwnerRecord:
        """发起 owner 迁移(CAS)。

        operation_id 语义(§9.23「一次真实进场 / owner 迁移使用一个稳定 operation_id」):

          - **空 = 由本权威铸造**。这是默认形态。调用方(allocator 签票点 / READY 交付点)
            无法自己保证稳定 —— 它们每次投递现铸一个 UUID,同一次进场的重连、重复交付、
            心跳自愈会写出**不同 operation**,幂等键失效。改由权威铸造后,同 exact 实例的
            重复投递在数据层原样返回既有记录(含原 operation_id),真实迁移才铸新的。

          - **非空 = 调用方持显式幂等键**(响应丢失后原样重试),必须是 canonical UUIDv4。

        source_revision 是 Hub assignment 的来源版本(INC-20260818-003)。
        0 = 调用方尚未滚上本协议(兼容窗);owner_type=BATTLE 时无意义,数据层会忽略。
        ★ 本层刻意**不**校验它的大小关系 —— 判定必须与写入落在**同一个行锁事务**里,
        放在这里做等于又造一个 TOCTOU。
        """
        if player_id == 0:
            _log_rejected(
                "BeginTransition",
                REASON_PLAYER_ID_REQUIRED,
                player_id,
                operation_id=operation_id,
                owner_type=owner_type,
                req_pod=target.pod_name,
            )
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")

        if operation_id and not placement.valid_operation_id(operation_id):
            _log_rejected(
                "BeginTransition",
                REASON_OPERATION_ID_INVALID,
                player_id,
                operation_id=operation_id,
                expect_epoch=expect_epoch,
            )
            raise errcode.PandoraError(
                errcode.ErrOwnerInvalidOperation, "operation_id must be canonical UUIDv4"
            )

        if owner_type not in (odata.OWNER_TYPE_HUB, odata.OWNER_TYPE_BATTLE):
            _log_rejected(
                "BeginTransition",
                REASON_OWNER_TYPE_INVALID,
                player_id,
                owner_type=owner_type,
                operation_id=operation_id,
                expect_owner_type_hub=odata.OWNER_TYPE_HUB,
                expect_owner_type_battle=odata.OWNER_TYPE_BATTLE,
            )
            raise errcode.PandoraError(
                errcode.ErrOwnerInvalidOperation, "owner_type must be HUB or BATTLE"
            )

        if not target.complete():
            # exact 实例身份不全 = 无法做 §9.22 的 exact 绑定。**逐项打出哪一项缺了** ——
            # 只说"身份不完整"会让排查退化成猜。
            _log_rejected(
                "BeginTransition",
                REASON_TARGET_INCOMPLETE,
                player_id,
                operation_id=operation_id,
                owner_type=owner_type,
                req_pod=target.pod_name,
                req_instance_uid=target.instance_uid,
                req_instance_epoch=target.instance_epoch,
                req_assignment_id=target.assignment_or_allocation_id,
                req_release_track=target.release_track,
            )
            raise errcode.PandoraError(
                errcode.ErrOwnerInvalidOperation, "target identity incomplete"
            )

        # 空 operation → 权威铸造。真实迁移时被写入记录并从此贯穿本次进场链;
        # 若数据层判定为同 exact 实例的重复投递(no-op 原样返回既有记录),
        # 这个新铸的值直接丢弃,**不落库**。
        if not operation_id:
            operation_id = placement.new_operation_id()

        return await self._repo.begin_transition(
            player_id,
            expect_epoch,
            operation_id,
            owner_type,
            target,
            source_revision,
            placement.DS_FENCE_SKEW_MARGIN_SECONDS,
        )

    async def admit(
        self, player_id: int, owner_epoch: int, operation_id: str, target: odata.OwnerTarget
    ) -> tuple[odata.OwnerRecord, int]:
        """准入提交(屏障 + exact 身份;幂等重放)。返回 (记录, retry_after_ms)。"""
        if player_id == 0 or owner_epoch == 0:
            reason = (
                REASON_OWNER_EPOCH_REQUIRED if player_id != 0 else REASON_PLAYER_ID_REQUIRED
            )
            _log_rejected(
                "Admit",
                reason,
                player_id,
                owner_epoch=owner_epoch,
                operation_id=operation_id,
                req_pod=target.pod_name,
            )
            raise errcode.PandoraError(
                errcode.ErrInvalidArg, "player_id/owner_epoch required"
            )

        # ★ Admit 的 operation_id **不允许为空**(与 BeginTransition 不同):
        # 准入是对"某一次已发起的迁移"的确认,没有 operation 就无从确认是哪一次。
        if not placement.valid_operation_id(operation_id):
            _log_rejected(
                "Admit",
                REASON_OPERATION_ID_INVALID,
                player_id,
                owner_epoch=owner_epoch,
                operation_id=operation_id,
            )
            raise errcode.PandoraError(
                errcode.ErrOwnerInvalidOperation, "operation_id must be canonical UUIDv4"
            )

        if not target.complete():
            _log_rejected(
                "Admit",
                REASON_TARGET_INCOMPLETE,
                player_id,
                owner_epoch=owner_epoch,
                operation_id=operation_id,
                req_pod=target.pod_name,
                req_instance_uid=target.instance_uid,
                req_instance_epoch=target.instance_epoch,
                req_assignment_id=target.assignment_or_allocation_id,
                req_release_track=target.release_track,
            )
            raise errcode.PandoraError(
                errcode.ErrOwnerInvalidOperation, "target identity incomplete"
            )

        return await self._repo.admit(player_id, owner_epoch, operation_id, target)

    async def renew_instance_lease(self, target: odata.OwnerTarget, lease_seconds: int) -> int:
        """实例租约续期(allocator 心跳代写)。返回生效截止时刻。

        续租要求 pod + uid;**instance_epoch 允许 0** —— hub 凭据不携带实例纪元,
        uid 全局唯一已足够;纪元守卫在数据层只对"双方都非零且不同"拒。
        分配 ID 是玩家维度信息,而租约是实例级,所以这里不要求它。
        """
        if not target.pod_name or not target.instance_uid:
            _log_rejected(
                "RenewInstanceLease",
                REASON_INSTANCE_INCOMPLETE,
                0,
                req_pod=target.pod_name,
                req_instance_uid=target.instance_uid,
                req_instance_epoch=target.instance_epoch,
                lease_seconds=lease_seconds,
            )
            raise errcode.PandoraError(
                errcode.ErrOwnerInvalidOperation, "instance identity incomplete"
            )
        if lease_seconds == 0:
            _log_rejected(
                "RenewInstanceLease",
                REASON_LEASE_SECONDS_MISSING,
                0,
                req_pod=target.pod_name,
                req_instance_uid=target.instance_uid,
            )
            raise errcode.PandoraError(errcode.ErrInvalidArg, "lease_seconds required")

        if lease_seconds > placement.DS_FENCE_LEASE_MAX_SECONDS:
            # ★ 协议上限**硬钳制**(§8 fence 契约):配置/调用方无法放大脑裂窗口。
            # 钳制是静默改写调用方意图,不可见就会把「租约比我要的短」误归因为续租掉链 ——
            # 所以必须留一条 WARN。
            plog.get().warning(
                "owner_lease_seconds_clamped",
                reason="lease_seconds_over_protocol_max",
                req_pod=target.pod_name,
                req_instance_uid=target.instance_uid,
                req_lease_seconds=lease_seconds,
                max_lease_seconds=placement.DS_FENCE_LEASE_MAX_SECONDS,
            )
            lease_seconds = placement.DS_FENCE_LEASE_MAX_SECONDS

        return await self._repo.renew_instance_lease(target, lease_seconds)

    async def release(
        self, player_id: int, owner_epoch: int, operation_id: str
    ) -> odata.OwnerRecord:
        """显式释放(登出/终局)。**迟到调用幂等 no-op**。"""
        if player_id == 0:
            _log_rejected(
                "Release",
                REASON_PLAYER_ID_REQUIRED,
                player_id,
                owner_epoch=owner_epoch,
                operation_id=operation_id,
            )
            raise errcode.PandoraError(errcode.ErrInvalidArg, "player_id required")
        if not placement.valid_operation_id(operation_id):
            _log_rejected(
                "Release",
                REASON_OPERATION_ID_INVALID,
                player_id,
                owner_epoch=owner_epoch,
                operation_id=operation_id,
            )
            raise errcode.PandoraError(
                errcode.ErrOwnerInvalidOperation, "operation_id must be canonical UUIDv4"
            )
        return await self._repo.release(player_id, owner_epoch, operation_id)

    async def run_transition_log_sweep(self, batch: int) -> int:
        """周期清理审计流水(§9.24)。"""
        return await self._repo.sweep_transition_log(self._cfg.log_retention_days, batch)
