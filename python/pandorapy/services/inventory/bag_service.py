"""背包域 gRPC service 层(pandora.bag.v1)—— 对应 Go 侧 internal/service/bag.go。

鉴权边界(五要件①身份,bag-domain.md §2):

  - 全部 RPC 是内部系统接口:调用方 = owner DS(经 :8444 DS 面直连,无玩家 JWT →
    caller_id==0);带玩家 JWT 的客户端调用一律拒(Envoy /pandora.bag.v1/ 前缀 403 双保险);
  - 五要件①:DSCallbackGuard 验签 DS 凭据令牌抽取 pod/uid(set_ds_guard 注入;
    mode=off/dev 时无身份,由授权器按契约 fail-closed 或 epoch 证明降级);
  - 五要件②:biz 层逐写查询 owner authority,record.target 与调用方身份全等校验;
    fencing(③)/ 额度(④)/ 审计(⑤)在 biz/data 层。

★ 业务失败一律走 **in-band code**(返回 response.code,gRPC status 保持 OK),
  与 Go 侧逐字一致 —— 调用方按 code 分支,不靠 gRPC status 猜。

★ 每处宽 except 之前必须先放行 asyncio.CancelledError:
  它在 3.8+ 是 BaseException,而 grpc.aio 正是**用取消**终止在途 handler。
  吞掉 = 停机时把取消映射成业务错误码返回一个正常响应,客户端每次滚动更新
  都会收到一批假失败。
"""

from __future__ import annotations

import asyncio

from pandora.bag.v1 import bag_pb2 as pb
from pandora.bag.v1 import bag_pb2_grpc as pbgrpc
from pandora.common.v1 import errcode_pb2 as commonpb

from pandorapy import dsauth
from pandorapy import errcode
from pandorapy import interceptors as pintercept
from pandorapy import log as plog
from pandorapy.services.inventory import bag_biz
from pandorapy.services.inventory import currency_biz as cbiz

GRPC_SERVICE_FULL_NAME = "pandora.bag.v1.BagService"

_ZERO_CALLER = bag_biz.DSCallerIdentity()


def _code_of(exc: BaseException) -> int:
    """内部异常 → in-band ErrCode(与 Go 的 errcode.As 同口径)。"""
    if isinstance(exc, errcode.PandoraError):
        return exc.code
    return errcode.ErrInternal


class BagService(pbgrpc.BagServiceServicer):
    """实现 pandora.bag.v1.BagService 的 5 个 RPC。"""

    def __init__(self, uc: bag_biz.BagUsecase) -> None:
        self._uc = uc
        # DS 回调令牌守卫(五要件①;None = ds_auth off,dev)。
        self._ds_guard: dsauth.DSCallbackGuard | None = None

    def set_ds_guard(self, guard: dsauth.DSCallbackGuard | None) -> None:
        """注入 DS 回调令牌守卫(main 按 ds_auth 配置装配;None-safe)。"""
        self._ds_guard = guard

    # ── 鉴权辅助 ──────────────────────────────────────────────────────────

    @staticmethod
    def _reject_client_caller(context) -> bool:  # noqa: ANN001
        """系统接口守卫:带玩家 JWT 的调用(caller_id>0)一律拒。"""
        return pintercept.extract_player_id(context) != 0

    def _resolve_ds_caller(self, context) -> tuple[bag_biz.DSCallerIdentity, int]:  # noqa: ANN001
        """验签抽取调用方 DS 身份(五要件①)。

        guard None/off/permissive 放行时返回零值身份(不冒充"已证明");验签失败 /
        范围不符 → (零值, code)。scope 不限定 hub/battle(两类 owner DS 都可写背包域;
        ②的 target 全等才是硬闸)。
        """
        if self._ds_guard is None:
            return _ZERO_CALLER, commonpb.OK
        _claims, cred, code = self._ds_guard.check_credential(
            context, dsauth.DSScope(require_token=True)
        )
        if code != 0:
            return _ZERO_CALLER, code
        if cred is None:
            return _ZERO_CALLER, commonpb.OK
        if cred.ds_type not in ("hub", "battle"):
            return _ZERO_CALLER, commonpb.OK
        return bag_biz.DSCallerIdentity(pod=cred.pod, uid=cred.instance_uid), commonpb.OK

    @staticmethod
    def _log_client_caller_denied(context, rpc: str, req_player_id: int) -> None:  # noqa: ANN001
        """记录一次玩家 JWT 直敲背包域内部接口的拒绝。

        ERR_PERMISSION_DENY 是 in-band code,handler 不抛错 → access log 只记 DEBUG。
        这是"玩家试图自助改背包"的直接信号,必须能主动发现。
        """
        plog.get().warning(
            "bag_client_caller_denied",
            rpc=rpc,
            caller_id=pintercept.extract_player_id(context),
            req_player_id=req_player_id,
        )

    # ── RPC ───────────────────────────────────────────────────────────────

    async def LoadBag(  # noqa: N802 —— gRPC 生成接口方法名
        self, request: pb.LoadBagRequest, context  # noqa: ANN001
    ) -> pb.LoadBagResponse:
        if self._reject_client_caller(context):
            self._log_client_caller_denied(context, "LoadBag", request.player_id)
            return pb.LoadBagResponse(code=commonpb.ERR_PERMISSION_DENY)
        caller, gcode = self._resolve_ds_caller(context)
        if gcode != commonpb.OK:
            return pb.LoadBagResponse(code=gcode)
        try:
            snapshot, tail, last_seq = await self._uc.load_bag(
                request.player_id, request.owner_epoch, caller
            )
            # 随身段权威有效容量(§5.3):base + 已购增量;checkpoint 内 capacity 仅回显不作数。
            capacities = await self._uc.carry_effective_capacities(request.player_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            return pb.LoadBagResponse(code=_code_of(exc))

        resp = pb.LoadBagResponse(code=commonpb.OK, last_journal_seq=last_seq)
        for cap in capacities:
            resp.effective_capacities.add(bag_type=cap.bag_type, capacity=cap.capacity)
        if snapshot:
            try:
                resp.snapshot.ParseFromString(snapshot)
            except Exception:  # noqa: BLE001
                return pb.LoadBagResponse(code=commonpb.ERR_INTERNAL)
        for _seq, payload in tail:
            try:
                resp.tail.add().ParseFromString(payload)
            except Exception:  # noqa: BLE001
                return pb.LoadBagResponse(code=commonpb.ERR_INTERNAL)
        return resp

    async def AppendJournal(  # noqa: N802
        self, request: pb.AppendJournalRequest, context  # noqa: ANN001
    ) -> pb.AppendJournalResponse:
        if self._reject_client_caller(context):
            self._log_client_caller_denied(context, "AppendJournal", request.player_id)
            return pb.AppendJournalResponse(code=commonpb.ERR_PERMISSION_DENY)
        caller, gcode = self._resolve_ds_caller(context)
        if gcode != commonpb.OK:
            return pb.AppendJournalResponse(code=gcode)
        try:
            acked = await self._uc.append_journal(
                request.player_id, request.owner_epoch, request.entries, caller
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            return pb.AppendJournalResponse(code=_code_of(exc))
        return pb.AppendJournalResponse(code=commonpb.OK, acked_seq=acked)

    async def SaveCheckpoint(  # noqa: N802
        self, request: pb.SaveCheckpointRequest, context  # noqa: ANN001
    ) -> pb.SaveCheckpointResponse:
        if self._reject_client_caller(context):
            self._log_client_caller_denied(context, "SaveCheckpoint", request.player_id)
            return pb.SaveCheckpointResponse(code=commonpb.ERR_PERMISSION_DENY)
        if not request.HasField("snapshot"):
            return pb.SaveCheckpointResponse(code=commonpb.ERR_INVALID_ARG)
        record = request.snapshot
        # 先用 protobuf 的无分配尺寸计算挡住超限载荷,避免为已知会被 biz 拒绝的
        # checkpoint 再分配一份 >256KiB 的序列化缓冲。biz 仍保留同一字节闸作第二道防线。
        if record.ByteSize() > bag_biz.BAG_CHECKPOINT_MAX_BYTES:
            return pb.SaveCheckpointResponse(code=commonpb.ERR_BAG_QUOTA_EXCEEDED)
        try:
            blob = record.SerializeToString()
        except Exception:  # noqa: BLE001
            return pb.SaveCheckpointResponse(code=commonpb.ERR_INTERNAL)
        caller, gcode = self._resolve_ds_caller(context)
        if gcode != commonpb.OK:
            return pb.SaveCheckpointResponse(code=gcode)
        try:
            await self._uc.save_checkpoint(
                request.player_id,
                request.owner_epoch,
                record,
                blob,
                request.covered_journal_seq,
                caller,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            return pb.SaveCheckpointResponse(code=_code_of(exc))
        return pb.SaveCheckpointResponse(code=commonpb.OK)

    async def PurchaseCapacity(  # noqa: N802
        self, request: pb.PurchaseCapacityRequest, context  # noqa: ANN001
    ) -> pb.PurchaseCapacityResponse:
        if self._reject_client_caller(context):
            self._log_client_caller_denied(context, "PurchaseCapacity", request.player_id)
            return pb.PurchaseCapacityResponse(code=commonpb.ERR_PERMISSION_DENY)
        caller, gcode = self._resolve_ds_caller(context)
        if gcode != commonpb.OK:
            return pb.PurchaseCapacityResponse(code=gcode)
        try:
            res = await self._uc.purchase_capacity(
                request.player_id, request.owner_epoch, request.bag_type, caller
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            return pb.PurchaseCapacityResponse(code=_code_of(exc))
        return pb.PurchaseCapacityResponse(
            code=commonpb.OK,
            purchases=res.purchases,
            extra=res.extra,
            effective_capacity=res.effective_capacity,
            cost=cbiz.currency_amount_proto(res.currency_kind, res.cost),
            balance=cbiz.currency_amount_proto(res.currency_kind, res.balance),
        )

    async def GetSections(  # noqa: N802
        self, request: pb.GetSectionsRequest, context  # noqa: ANN001
    ) -> pb.GetSectionsResponse:
        if self._reject_client_caller(context):
            self._log_client_caller_denied(context, "GetSections", request.player_id)
            return pb.GetSectionsResponse(code=commonpb.ERR_PERMISSION_DENY)
        try:
            sections = await self._uc.get_sections(request.player_id, list(request.bag_types))
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            return pb.GetSectionsResponse(code=_code_of(exc))
        return pb.GetSectionsResponse(code=commonpb.OK, sections=sections)
