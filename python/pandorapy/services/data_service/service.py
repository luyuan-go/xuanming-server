"""data_service gRPC service 层 —— 对应 Go 侧 internal/service/data.go。

职责(与 Go 侧逐条对应):
  - 实现 DataServiceServicer
  - proto Request/Response ↔ biz 入参/出参互转
  - errcode → pandora.common.v1.ErrCode 1:1 映射(纯数值)

★ 身份来源:**取请求体里的 player_id,不从 JWT override** ——
  这是本仓唯一一处刻意与"player_id 一律取鉴权上下文"相反的服务,照抄 Go 的注释:

      「data_service 是内网服务-to-服务网关(不经 Envoy / 不直接暴露给玩家),
        player_id 取请求体字段,不从 JWT override。由内网 RPC 黑白名单限制调用方。」

  调用方是 player / inventory 这些内网服务,它们**代表别的玩家**读写数据,
  自己并不持有那个玩家的 JWT。若照搬客户端面服务的写法去 extract_player_id,
  拿到的恒为 0 → 每个 RPC 都 ERR_UNAUTHORIZED,整条玩家数据链直接断掉。
  与之配套,server 层也**不挂** auth_required(Go 的 NewGRPCServer 没有 AuthRequired)。

⚠️ 返回值形态必须和 Go 侧一致:业务失败**不**抛 gRPC 错误,而是返回
   `Response(code=ErrXxx)` 且 gRPC status 为 OK。客户端读的是 body 里的 code。
   若改成 context.abort(),调用方会走到完全不同的错误分支 —— 这是最容易在迁移中
   悄悄改掉的语义。
"""

from __future__ import annotations

import grpc
from pandora.common.v1 import errcode_pb2
from pandora.data_service.v1 import data_service_pb2, data_service_pb2_grpc

from pandorapy import errcode
from pandorapy.services.data_service import biz as dbiz


class DataService(data_service_pb2_grpc.DataServiceServicer):
    """实现 DataServiceServicer。对应 Go 的 service.DataService。"""

    __slots__ = ("_uc",)

    def __init__(self, usecase: dbiz.DataUsecase) -> None:
        self._uc = usecase

    async def ReadPlayer(  # noqa: N802 —— gRPC 生成的方法名,必须照抄
        self,
        request: data_service_pb2.ReadPlayerRequest,
        context: grpc.aio.ServicerContext,
    ) -> data_service_pb2.ReadPlayerResponse:
        """cache-aside 读玩家数据。无数据 → ERR_NOT_FOUND。"""
        if request.player_id == 0:
            return data_service_pb2.ReadPlayerResponse(code=errcode_pb2.ERR_INVALID_ARG)
        try:
            pd = await self._uc.read_player(request.player_id)
        except Exception as exc:  # noqa: BLE001 —— 与 Go 侧一样把 error 映射成 code
            return data_service_pb2.ReadPlayerResponse(code=_to_proto_code(exc))
        if pd is None:
            # ★ "读到了但没有这行"与"读失败"必须分开:Go 的 (nil,false,nil) 走这一支。
            # 合并成 ERR_INTERNAL 会让调用方把"新玩家还没建档"当成源库故障去重试。
            return data_service_pb2.ReadPlayerResponse(code=errcode_pb2.ERR_NOT_FOUND)
        return data_service_pb2.ReadPlayerResponse(code=errcode_pb2.OK, data=pd)

    async def WritePlayer(  # noqa: N802
        self,
        request: data_service_pb2.WritePlayerRequest,
        context: grpc.aio.ServicerContext,
    ) -> data_service_pb2.WritePlayerResponse:
        """乐观锁版本写。版本不匹配 → ERR_DATA_VERSION_MISMATCH。

        更新(data.version>0)必须带非空 update_mask,只更新掩码内的业务列
        (滚动升级时旧副本不清零新列);空掩码更新 → ERR_INVALID_ARG。
        新建(version==0)整条 INSERT,忽略掩码。掩码校验本身在 biz,这里只做入参存在性检查。
        """
        # ★ 判据是 HasField("data") 而不是 `not request.data`:proto3 的 message 字段
        # 读出来永远是非 None 的默认实例,`not request.data` 对"显式传了空 data"和
        # "根本没传 data"都为真但语义不同;而 Go 的 GetData()==nil 只在**没传**时成立。
        # 两者最终都落到 ERR_INVALID_ARG,所以用哪个都不改结果 —— 用 HasField 是为了
        # 让这行代码读起来就是 Go 那行的意思,而不是碰巧等价。
        if not request.HasField("data") or request.data.player_id == 0:
            return data_service_pb2.WritePlayerResponse(code=errcode_pb2.ERR_INVALID_ARG)
        try:
            new_version = await self._uc.write_player(
                request.data, list(request.update_mask.paths)
            )
        except Exception as exc:  # noqa: BLE001
            return data_service_pb2.WritePlayerResponse(code=_to_proto_code(exc))
        return data_service_pb2.WritePlayerResponse(
            code=errcode_pb2.OK, new_version=new_version
        )

    async def InvalidateCache(  # noqa: N802
        self,
        request: data_service_pb2.InvalidateCacheRequest,
        context: grpc.aio.ServicerContext,
    ) -> data_service_pb2.InvalidateCacheResponse:
        """主动删缓存(供上层在外部直写 DB 后强制失效)。"""
        if request.player_id == 0:
            return data_service_pb2.InvalidateCacheResponse(
                code=errcode_pb2.ERR_INVALID_ARG
            )
        try:
            await self._uc.invalidate_cache(request.player_id)
        except Exception as exc:  # noqa: BLE001
            return data_service_pb2.InvalidateCacheResponse(code=_to_proto_code(exc))
        return data_service_pb2.InvalidateCacheResponse(code=errcode_pb2.OK)


def _to_proto_code(err: BaseException) -> int:
    """errcode → proto enum,1:1 数值映射。对应 Go 的 toProtoCode。

    数值一致性由 tools/gen_errcode.py 从 Go 源码生成 + tests 里的 parity 测试保证。
    """
    return errcode.as_code(err)
