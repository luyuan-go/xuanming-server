"""battle_result 的 kafka 消费 handler —— 对应 Go 侧 internal/biz/consumer.go。

订阅两个 topic,各用一个 kafkax.KeyOrderedConsumer(main 层装配),handler 在此定义:
解 proto → 调 usecase。

★ 解码失败必须包成 `kafkax.poison(...)`:毒丸重试无意义,直接进 DLQ。
  抛普通异常的话会先白跑完 3 次重试才进 DLQ —— 更糟的是,如果连 DLQ 都投不出去,
  该 partition 会永久卡在这条坏消息上,消费组看着活着而 lag 一直涨。

★ trace_id:kafkax **不透传 trace_id**(pkg/kafkax 里没有任何 trace 接线),消费侧
  上下文恒空。「打完没结算」的另一半答案就在这条链上,必须能按 match_id 查、
  按 trace_id 串起本次消费产生的全部下游写(§9.8 / §11.3 R3)。所以在 handler 里
  现铸一个 trace_id。同一 offset 重投会拿到新 trace_id;**跨重试的关联键是 match_id**。
"""

from __future__ import annotations

import uuid

from pandora.battle.v1 import battle_pb2
from pandora.ds.v1 import allocator_pb2 as ds_pb2

from pandorapy import errcode, kafkax
from pandorapy import log as plog
from pandorapy.protoenum import enum_name
from pandorapy.services.battle_result import biz as bbiz


def battle_result_handler(uc: bbiz.BattleResultUsecase):  # noqa: ANN201
    """旧 pandora.battle.result 的消费 handler(幂等落库 + MMR)。

    ⚠️ 该消息**不携带 Model-B credential**。authority_mode=redis 时
    `Config.validate_redis_authority_ingress()` 会在启动期禁止订阅本 topic ——
    结算唯一入口改为受 Guard + Redis active + receipt 保护的同步 ReportResult RPC。
    走到这里说明当前是 legacy/off 档。
    """

    async def _handle(msg) -> None:  # noqa: ANN001 —— kafka-python ConsumerRecord
        result = battle_pb2.BattleResult()
        try:
            result.ParseFromString(msg.value or b"")
        except Exception as exc:  # noqa: BLE001
            raise kafkax.poison(
                errcode.PandoraError(
                    errcode.ErrBattleResultDecode,
                    "decode battle.result offset=%d: %s",
                    msg.offset,
                    exc,
                )
            ) from exc
        token = plog.bind_trace_id(str(uuid.uuid4()))
        try:
            await uc.report_result(result, 0)
        finally:
            token.var.reset(token)

    return _handle


def ds_lifecycle_handler(uc: bbiz.BattleResultUsecase):  # noqa: ANN201
    """pandora.ds.lifecycle 的消费 handler。

    **只处理 ABANDONED**(DS 崩溃补偿,不变量 §4),其余阶段忽略 ——
    ALLOCATED / RELEASED 由 ds_allocator 自己的链路处理,在这里重复处理会重复写补偿记录。
    """

    async def _handle(msg) -> None:  # noqa: ANN001
        evt = ds_pb2.DSLifecycleEvent()
        try:
            evt.ParseFromString(msg.value or b"")
        except Exception as exc:  # noqa: BLE001
            raise kafkax.poison(
                errcode.PandoraError(
                    errcode.ErrBattleResultDecode,
                    "decode ds.lifecycle offset=%d: %s",
                    msg.offset,
                    exc,
                )
            ) from exc
        if evt.phase != ds_pb2.DS_LIFECYCLE_PHASE_ABANDONED:
            plog.get().debug(
                "ds_lifecycle_ignored",
                phase=enum_name(ds_pb2.DSLifecyclePhase, evt.phase),
                match_id=evt.match_id,
            )
            return
        token = plog.bind_trace_id(str(uuid.uuid4()))
        try:
            plog.get().info(
                "ds_lifecycle_abandoned_received",
                match_id=evt.match_id,
                players=len(evt.player_ids),
                map_id=evt.map_id,
                game_mode=evt.game_mode,
                ts_ms=evt.ts_ms,
                kafka_offset=msg.offset,
            )
            await uc.handle_abandoned(
                evt.match_id,
                list(evt.player_ids),
                evt.map_id,
                evt.game_mode,
                evt.ts_ms,
            )
        finally:
            token.var.reset(token)

    return _handle
