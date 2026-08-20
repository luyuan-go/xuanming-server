"""kafka 消费 handler —— 对应 Go 侧 internal/biz/consumer.go。

player 订阅 `pandora.player.update`(battle_result 结算后发),解 proto → 幂等 UpdateMMR
(idempotency_key=match_id,不变量 §2)。

★ `pandora.player.update` 是**单事件类型 topic**(§21):只承载 PlayerUpdateEvent。
  旧 player 副本消费它时**不看 event_type header**,直接按 MMR 事件解码 —— 所以往这个
  topic 上加任何新 event_type 都会静默污染段位。player 域新增事件一律开新 topic
  (经验事件走 pandora.player.experience 就是这么来的)。

★ 本 handler 仍防御性校验 event_type header,三档语义逐条对齐 Go:
    缺失 / "0"          → 旧事件,正常处理(兼容旧 producer 不写 header);
    合法非 0            → 不属本消费者的未来事件,跳过并告警(**不得**按 MMR 误解码);
    存在但非法(非数字)→ **毒丸进 DLQ 留证**,绝不能降级当旧事件解码。
  第三档最要紧:降级解码等于把一个来路不明的 payload 按 MMR 事件解出来加进玩家段位。

★ 三条"跳过"分支都是 `return`(ack)而不是抛错:它们是**确定性不可处理**的消息,
  重试多少次结果都一样,不 ack 会让该分区永久卡死。但每条都带 offset + key 打 WARN
  —— 静默跳过才能事后按玩家 join(kafka key = player_id,§9.9)。
"""

from __future__ import annotations

from pandora.player.v1 import player_pb2 as ppb

from pandorapy import kafkax
from pandorapy import log as plog


def _header_event_type(headers) -> tuple[int, bool]:  # noqa: ANN001
    """从 kafka headers 解析 event_type。

    返回 (值, 是否合法):缺失 → (0, True)(兼容旧 producer 不写 header);
    存在且为合法非负整数 → (v, True);存在但非法(非数字/负数/溢出)→ (0, False),
    调用方必须按毒丸处理。
    """
    for item in headers or ():
        try:
            key, value = item[0], item[1]
        except (TypeError, IndexError):
            continue
        if key != kafkax.HEADER_EVENT_TYPE:
            continue
        raw = value.decode("utf-8", "replace") if isinstance(value, (bytes, bytearray)) else str(value)
        try:
            parsed = int(raw)
        except ValueError:
            return 0, False
        # Go 用 ParseUint(…, 10, 32):负数与溢出都是"非法",不是"当 0 处理"。
        if parsed < 0 or parsed > 0xFFFFFFFF:
            return 0, False
        return parsed, True
    return 0, True


def player_update_handler(uc):  # noqa: ANN001, ANN201 —— biz.PlayerUsecase
    """返回 pandora.player.update 的消费 handler(幂等 UpdateMMR)。"""

    async def _handle(msg) -> None:  # noqa: ANN001
        logger = plog.get()
        event_type, ok = _header_event_type(getattr(msg, "headers", None))
        if not ok:
            raise kafkax.poison(
                f"malformed event_type header on player.update offset={msg.offset}"
            )
        if event_type != int(ppb.PLAYER_PUSH_EVENT_TYPE_LEGACY_UPDATE):
            key = msg.key.decode("utf-8", "replace") if isinstance(msg.key, (bytes, bytearray)) else str(msg.key or "")
            logger.warning(
                "player_update_unexpected_event_type_skipped",
                event_type=event_type,
                offset=msg.offset,
                key=key,
            )
            return

        evt = ppb.PlayerUpdateEvent()
        try:
            evt.ParseFromString(msg.value)
        except Exception as exc:  # noqa: BLE001 —— proto 解码失败 = 毒丸
            raise kafkax.poison(
                f"decode player.update offset={msg.offset}: {exc}"
            ) from exc

        if evt.player_id == 0:
            logger.warning("player_update_missing_player_id", offset=msg.offset)
            return
        if evt.match_id == 0:
            # 幂等键缺失:无法保证不变量 §2,丢弃(battle_result 正常路径必带 match_id)。
            logger.warning(
                "player_update_missing_match_id", player_id=evt.player_id, offset=msg.offset
            )
            return

        # 段位池随事件带来(battle_result 从 canonical BattleStorageRecord 定格值填);
        # 空 = 旧 battle_result 或旧对局,由 update_mmr 归一到默认池(§9.21)。
        await uc.update_mmr(
            evt.player_id, evt.mmr_delta, evt.reason, str(evt.match_id), evt.rating_pool
        )

    return _handle
