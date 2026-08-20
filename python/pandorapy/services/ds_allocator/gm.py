"""ds_allocator 的 GM 指令队列 —— 对应 Go 侧
`services/battle/ds_allocator/internal/gm/gm.go`(524 行)。

它是一条 **Redis List 队列**:GM 后台调 `SendCommand` 入队,战斗 DS 每帧调
`PollCommands` 出队执行,执行完调 `AckCommand` 回报结果。可选地,`SendCommand`
可以带一个 metadata 头**同步等待**执行回执(GM 面板要看到"发下去了没生效")。

═══════════════════════════════════════════════════════════════════════════════
与 Go 的形变
═══════════════════════════════════════════════════════════════════════════════

形变 1:**没有 ctx**。Go 的 `waitForExecutionAck` 用 `context.WithTimeout(ctx, 15s)`
    同时承载"调用方断连"和"本地 15s 上限"。Python 侧:
      - 调用方断连 → grpc.aio 直接向 handler 抛 `CancelledError`,本模块**原样上抛**
        (§16.7 / rule 4:取消是控制流,不是业务失败);
      - 本地 15s 上限 → 用 `time.monotonic()` 算 deadline,BLPOP 按 1s 切片轮询。
    ★ 因此 Go 的 `gm_command_execution_wait_canceled`(→ ERR_CANCELED)在 Python 侧
      **不会**产生一条 in-band 应答:handler 被取消时应答本来也发不出去。事件名保留
      在 `EVENT_WAIT_CANCELED` 常量里只为跨语言词表完整,不是死代码占位。

形变 2:**Lua 脚本用 `register_script`**。Go 的 `redis.NewScript` 走 EVALSHA + 首次
    NOSCRIPT 回退 EVAL;redis-py 的 `Script` 对象语义相同。脚本正文**逐字节照抄**,
    一个空格都没动 —— 它的 SHA 是集群侧缓存键,改格式就是换一份脚本。

形变 3:**`-bin` trailer 必须是 bytes**。Go 的 `metadata.Pairs` 对 `-bin` 后缀 key
    接受 string;Python grpc 要求 `-bin` 的值是 `bytes`,故显式 `.encode("utf-8")`。

形变 4:**Redis 返回 bytes**。Go 的 `.Result()` 回 string;redis-py(decode_responses
    默认关)回 `bytes`。凡与常量比较处一律先 decode,**不是**把常量写成 bytes ——
    后者会让 `executionAckPendingValue` 这类跨语言词表在 Python 侧长得不一样。
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from typing import Any, Protocol, runtime_checkable

import redis.exceptions as redis_exceptions
from pandora.common.v1 import errcode_pb2 as commonpb
from pandora.ds.v1 import allocator_pb2 as dspb  # noqa: F401 —— 见 BattleLivenessChecker 注释
from pandora.gm.v1 import gm_pb2 as gmpb
from pandora.gm.v1 import gm_pb2_grpc as gmgrpc

from pandorapy import errcode
from pandorapy import log as plog
from pandorapy.services.ds_allocator.battle_auth import BattleCredentialIdentity

# ── 常量(与 Go 逐字同值)─────────────────────────────────────────────────────

#: `PollCommands` 未指定 max 时的默认批量。
DEFAULT_POLL_MAX = 16
#: `PollCommands` 单次出队硬上限(§9 不变量 18 的读取侧上限)。
MAX_POLL_MAX = 64
#: 单个 match 的队列长度上限。LTRIM 保留**最新** MAX_QUEUE_LEN 条。
#:
#: ★ 这是写入侧总量上限:GM 面板狂点 / 脚本刷指令时,队列不会无限涨。超出部分被
#:   LTRIM 丢弃的是**最旧**的指令 —— 与 Go 的 `LTrim(0, maxQueueLen-1)` 同语义
#:   (LPUSH 从头插,索引 0 是最新)。
MAX_QUEUE_LEN = 256
#: 队列 key 的 TTL。对局最长也就几十分钟,30min 后残留队列自然消失。
QUEUE_TTL_SEC = 30 * 60.0
#: 同步等待执行回执的最长时间。
#:
#: ★ 有界是硬要求(§9 不变量 19/20):GM 面板是人在等,DS 崩了就永远不会 Ack,
#:   没有这道上限的话那条 gRPC 会挂到调用方自己的 deadline(或永远)。
EXECUTION_ACK_WAIT_TIMEOUT_SEC = 15.0
#: 执行回执在 Redis 里的保留期(远大于等待窗口,让 ACK-loss 重放仍能读到结果)。
EXECUTION_ACK_TTL_SEC = 10 * 60.0
#: BLPOP 的单次阻塞切片。切成 1s 是为了让本地 deadline 判定的精度不依赖 Redis。
EXECUTION_ACK_BLOCK_SLICE_SEC = 1.0
#: `AddItemCommand.bag_type` 的合法上限(闭区间 [0, 3])。
MAX_BAG_TYPE = 3
#: Ack 回传消息的字节上限(截断,不拒绝)。
MAX_ACK_MESSAGE_BYTES = 512
#: 幂等键的字节上限。
MAX_ACK_IDEMPOTENCY_KEY_BYTES = 64

#: 调用方用它请求"同步等执行回执"。
WAIT_EXECUTION_ACK_METADATA_KEY = "x-pandora-gm-wait-execution-ack"
WAIT_EXECUTION_ACK_METADATA_VALUE = "1"
#: 执行失败时,失败原文经这个二进制 trailer 回传(避免污染 in-band code)。
EXECUTION_ACK_MESSAGE_METADATA_KEY = "x-pandora-gm-execution-message-bin"
#: 结果键的"已入队、尚未执行"占位值。
EXECUTION_ACK_PENDING_VALUE = "pending"

# ── 日志事件名(跨语言固定词表,逐字节与 Go 相同)──────────────────────────
EVENT_LIVENESS_CHECK_FAILED = "gm_command_liveness_check_failed"
EVENT_MATCH_NOT_FOUND = "gm_command_match_not_found"
EVENT_MARSHAL_FAILED = "gm_command_marshal_failed"
EVENT_ENQUEUE_FAILED = "gm_command_enqueue_failed"
EVENT_ENQUEUED = "gm_command_enqueued"
EVENT_UNMARSHAL_FAILED = "gm_command_unmarshal_failed"
EVENT_DELIVERED = "gm_commands_delivered"
EVENT_ACK_CONFLICT = "gm_command_ack_conflict"
EVENT_ACK_STATE_CORRUPT = "gm_command_ack_state_corrupt"
EVENT_ACKED = "gm_command_acked"
EVENT_MARKER_MISSING = "gm_command_execution_marker_missing"
EVENT_SIGNAL_INVALID = "gm_command_execution_signal_invalid"
EVENT_RESULT_CORRUPT = "gm_command_execution_result_corrupt"
EVENT_EXECUTION_FAILED = "gm_command_execution_failed"
EVENT_EXECUTION_CONFIRMED = "gm_command_execution_confirmed"
#: 见模块头形变 1:Python 侧取消直接上抛,这条事件不产生 in-band 应答。
EVENT_WAIT_CANCELED = "gm_command_execution_wait_canceled"
EVENT_WAIT_TIMEOUT = "gm_command_execution_wait_timeout"
EVENT_WAIT_FAILED = "gm_command_execution_wait_failed"


# ── Redis key ────────────────────────────────────────────────────────────────


def queue_key(match_id: int) -> str:
    """GM 指令队列 key。对应 Go 的 `queueKey`。

    ★ `{match_id}` 的花括号是 **Redis Cluster hashtag**:它让同一 match 的队列 key、
      结果 key、信号 key 落在同一个 slot,`store_execution_ack` 那段 Lua 才能同时
      写两个 key(跨 slot 的多 key 脚本在集群上直接报错)。
    """
    return "pandora:gm:queue:{" + str(match_id) + "}"


def execution_ack_key_prefix(match_id: int, idempotency_key: str) -> str:
    """执行回执的 key 前缀(`+":result"` / `+":signal"`)。对应 Go 的同名函数。"""
    return "pandora:gm:ack:{" + str(match_id) + "}:" + idempotency_key


#: 提交执行回执的原子脚本。对应 Go 的 `storeExecutionAckScript`。
#:
#: 为什么必须是 Lua 而不是 GET + SET(照抄 Go 的理由):
#:   ① "当前值必须是 pending" 与 "写入最终结果" 之间不能有窗口 —— 否则两台 DS
#:      同时 Ack 同一条指令会双写,后写的覆盖先写的,而调用方已经拿走了先写的结果;
#:   ② 写结果与 LPUSH 信号必须同生共死 —— 只写结果不推信号,等待方会一直 BLPOP 到
#:      超时(GM 面板显示"超时"而指令其实已生效);只推信号不写结果,等待方 GET 回
#:      pending 会当成信号无效。
#: 返回值三态:false=键不存在(过期/从未入队)、ARGV[2]=本次写入成功、其它=当前值
#: (说明已被别人 Ack,调用方按冲突处理)。
STORE_EXECUTION_ACK_SCRIPT = """
local current = redis.call('GET', KEYS[1])
if not current then
    return false
end
if current == ARGV[1] then
    redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
    redis.call('LPUSH', KEYS[2], ARGV[2])
    redis.call('PEXPIRE', KEYS[2], ARGV[3])
    return ARGV[2]
end
return current
"""


# ── 注入口 ───────────────────────────────────────────────────────────────────


@runtime_checkable
class BattleLivenessChecker(Protocol):
    """"这个 match 现在还有活着的战斗镜像吗"。对应 Go 的 `BattleLivenessChecker`。

    `AllocatorUsecase`(经 repo)满足它:`get_battle(match_id)` 不存在时返回 `None`
    (Go 是 `(nil, false, nil)`)。
    """

    async def get_battle(self, match_id: int) -> Any | None: ...


@runtime_checkable
class BattleAuthRepo(Protocol):
    """Model B 的权威仓,只用到出队与 active 校验两件事。"""

    async def check_active(self, match_id: int, ident: BattleCredentialIdentity) -> None: ...

    async def pop_commands_if_active(
        self,
        match_id: int,
        ident: BattleCredentialIdentity,
        queue: str,
        count: int,
    ) -> list[bytes]: ...


@runtime_checkable
class BattleCredentialGuard(Protocol):
    """DS 回调令牌守卫(结构化接口)。返回 **in-band code**,不抛异常。"""

    def check(self, context: Any, scope: Any) -> int: ...

    def check_credential(
        self, context: Any, scope: Any
    ) -> tuple[Any | None, Any | None, int]: ...


# ── 辅助 ─────────────────────────────────────────────────────────────────────


def _to_proto_code(exc: BaseException | None) -> int:
    return errcode.as_code(exc)


def _decode(raw: Any) -> str:
    """Redis 回值 → str(见模块头形变 4)。"""
    if isinstance(raw, bytes):
        return raw.decode("utf-8", errors="replace")
    return str(raw)


def bounded_ack_message(message: str) -> str:
    """把 DS 回传的失败原文收敛成"可安全落日志 / 可安全放 trailer"的短串。

    对应 Go 的 `boundedAckMessage`。两件事,缺一不可:

      ① **控制字符归一成空格**(<0x20 与 0x7f)。DS 回传的是任意字节,含换行 / 回车 /
         ANSI 转义。原样进结构化日志会把一条日志撕成多行(Loki 侧从此对不上),
         原样进 gRPC trailer 则违反 HTTP/2 头部字节约束。
      ② **截断到 512 字节**,且**保持 UTF-8 完整**。按字符截会在多字节汉字中间切断,
         产出的半个码点会让下游 JSON 编码器抛错 —— 一条"执行失败"的诊断信息反而
         变成一次 500。
    """
    if message == "":
        return ""
    cleaned = "".join(
        " " if (ord(ch) < 0x20 or ord(ch) == 0x7F) else ch for ch in message
    )
    raw = cleaned.encode("utf-8")
    if len(raw) <= MAX_ACK_MESSAGE_BYTES:
        return cleaned
    # errors="ignore" 丢弃末尾被切断的不完整码点(而不是替换成 U+FFFD):
    # 这里要的是"少一个字",不是"多一个问号"。
    return raw[:MAX_ACK_MESSAGE_BYTES].decode("utf-8", errors="ignore")


def _wants_execution_ack(context) -> bool:  # noqa: ANN001
    """调用方是否请求同步等待执行回执。对应 Go 的 metadata 判据。"""
    try:
        md = context.invocation_metadata()
    except Exception:  # noqa: BLE001 —— 测试桩 / 无 metadata 的调用
        return False
    for key, value in md or ():
        if isinstance(value, bytes):
            continue
        if (
            key.lower() == WAIT_EXECUTION_ACK_METADATA_KEY
            and value == WAIT_EXECUTION_ACK_METADATA_VALUE
        ):
            return True
    return False


# ── GmService ────────────────────────────────────────────────────────────────


class GmService(gmgrpc.GmServiceServicer):
    """GM 指令队列的 3 个 RPC。对应 Go 的 `gm.Service`。"""

    __slots__ = (
        "_rdb",
        "_store_ack",
        "_battle_checker",
        "_ds_guard",
        "_battle_auth",
        "_model_b",
    )

    def __init__(self, rdb: Any) -> None:
        self._rdb = rdb
        # register_script 不产生 IO,只是构造一个带 SHA 的可调用对象(等价 Go 的
        # redis.NewScript)。放在 __init__ 里,避免每次 Ack 都重算 SHA。
        self._store_ack = rdb.register_script(STORE_EXECUTION_ACK_SCRIPT)
        self._battle_checker: BattleLivenessChecker | None = None
        self._ds_guard: BattleCredentialGuard | None = None
        self._battle_auth: BattleAuthRepo | None = None
        self._model_b = False

    def set_battle_checker(self, checker: BattleLivenessChecker | None) -> None:
        """注入对局存活检查器。对应 Go 的 `SetBattleChecker`。"""
        self._battle_checker = checker

    def set_ds_callback_guard(self, guard: BattleCredentialGuard | None) -> None:
        """注入 DS 回调令牌守卫。对应 Go 的 `SetDSCallbackGuard`。"""
        self._ds_guard = guard

    def enable_redis_authority(self, repo: BattleAuthRepo | None) -> None:
        """切到 Model B(出队 / Ack 都要过 active 校验)。对应 Go 的 `EnableRedisAuthority`。

        Raises:
            errcode.PandoraError(ErrInvalidState): repo 为 None。
                ★ 不允许"开了 Model B 但没给仓"静默降级成 legacy:那会让一台已被
                  吊销的 DS 继续拿到 GM 指令,而部署方以为权威已生效。
        """
        if repo is None:
            raise errcode.PandoraError(
                errcode.ErrInvalidState, "gm Model B requires battle auth repo"
            )
        self._battle_auth = repo
        self._model_b = True

    # ── RPC 1:SendCommand ────────────────────────────────────────────────

    async def SendCommand(self, request, context):  # noqa: N802, ANN001, C901, PLR0911, PLR0912
        """GM 后台下发一条指令(入队)。"""
        match_id = request.match_id
        if match_id == 0:
            return gmpb.SendCommandResponse(code=commonpb.ERR_INVALID_ARG)
        idempotency_key = str(uuid.uuid4())
        command = gmpb.GmCommand(
            idempotency_key=idempotency_key,
            match_id=match_id,
            created_at_ms=int(time.time() * 1000),
        )
        # 目前只支持 add_item 一种 payload。`WhichOneof` 而不是 `HasField`:
        # 后者在 oneof 未设置时对标量分支会抛,前者恒回 None。
        if request.WhichOneof("payload") != "add_item":
            return gmpb.SendCommandResponse(code=commonpb.ERR_INVALID_ARG)
        add_item = request.add_item
        if (
            add_item.player_id == 0
            or add_item.config_id == 0
            or add_item.count <= 0
            or add_item.bag_type < 0
            or add_item.bag_type > MAX_BAG_TYPE
        ):
            return gmpb.SendCommandResponse(code=commonpb.ERR_INVALID_ARG)
        command.add_item.CopyFrom(add_item)

        # 存活检查:对一个已经结束 / 从不存在的 match 入队,指令会烂在队列里到 TTL。
        if self._battle_checker is not None:
            try:
                battle = await self._battle_checker.get_battle(match_id)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                # fail-open:Redis 抖动不该让 GM 完全不能下发指令(指令本身还有
                # 幂等键 + DS 侧校验兜底)。这与"鉴权 fail-closed"不矛盾 ——
                # 这里判的是"对局在不在",不是"你有没有权限"。
                plog.get().warning(
                    EVENT_LIVENESS_CHECK_FAILED,
                    match_id=match_id,
                    err=str(exc),
                    hint="fail-open,仍入队",
                )
            else:
                if battle is None:
                    plog.get().warning(
                        EVENT_MATCH_NOT_FOUND,
                        match_id=match_id,
                        hint="无活跃战斗镜像,拒绝入队(match_id 是否写错/对局已结束?)",
                    )
                    return gmpb.SendCommandResponse(code=commonpb.ERR_NOT_FOUND)

        try:
            payload = command.SerializeToString()
        except Exception as exc:  # noqa: BLE001 —— protobuf 序列化失败
            plog.get().error(EVENT_MARSHAL_FAILED, match_id=match_id, err=str(exc))
            return gmpb.SendCommandResponse(code=commonpb.ERR_INTERNAL)

        wait_for_ack = _wants_execution_ack(context)
        result_key = execution_ack_key_prefix(match_id, idempotency_key) + ":result"
        queue = queue_key(match_id)
        try:
            async with self._rdb.pipeline(transaction=True) as pipe:
                pipe.lpush(queue, payload)
                # LTRIM 保留最新 MAX_QUEUE_LEN 条:写入侧总量上限(§9 不变量 18)。
                pipe.ltrim(queue, 0, MAX_QUEUE_LEN - 1)
                pipe.expire(queue, int(QUEUE_TTL_SEC))
                if wait_for_ack:
                    # 占位必须与入队**同一个事务**:先入队后占位的话,DS 可能在两条
                    # 命令之间就把指令执行完并 Ack —— 那次 Ack 会因为结果键还不存在
                    # 而被判成"marker missing",GM 面板看到失败而指令其实已生效。
                    pipe.set(
                        result_key,
                        EXECUTION_ACK_PENDING_VALUE,
                        px=int(EXECUTION_ACK_TTL_SEC * 1000),
                    )
                await pipe.execute()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().error(EVENT_ENQUEUE_FAILED, match_id=match_id, err=str(exc))
            return gmpb.SendCommandResponse(code=commonpb.ERR_INTERNAL)

        plog.get().info(
            EVENT_ENQUEUED,
            match_id=match_id,
            idempotency_key=idempotency_key,
            type="add_item",
            player_id=add_item.player_id,
            config_id=add_item.config_id,
            count=add_item.count,
            wait_ack=wait_for_ack,
        )
        if not wait_for_ack:
            return gmpb.SendCommandResponse(
                code=commonpb.OK, idempotency_key=idempotency_key
            )
        code = await self._wait_for_execution_ack(
            context, match_id, idempotency_key
        )
        return gmpb.SendCommandResponse(code=code, idempotency_key=idempotency_key)

    # ── RPC 2:PollCommands ───────────────────────────────────────────────

    async def PollCommands(self, request, context):  # noqa: N802, ANN001, C901, PLR0911
        """战斗 DS 出队(每帧调)。"""
        match_id = request.match_id
        if match_id == 0:
            return gmpb.PollCommandsResponse(code=commonpb.ERR_INVALID_ARG)
        # 单次出队上限:DS 传 0 用默认,传超界钳到 MAX_POLL_MAX(§9 不变量 18 读取侧)。
        count = request.max
        if count <= 0:
            count = DEFAULT_POLL_MAX
        if count > MAX_POLL_MAX:
            count = MAX_POLL_MAX

        queue = queue_key(match_id)
        if self._model_b:
            if request.ds_pod_name == "":
                # Model B 下 pod 名是凭据绑定的一部分,不能省。
                return gmpb.PollCommandsResponse(code=commonpb.ERR_INVALID_ARG)
            ident, code = self._model_b_credential(
                context, match_id, request.ds_pod_name
            )
            if code != 0:
                return gmpb.PollCommandsResponse(code=code)
            assert self._battle_auth is not None  # _model_b 为真时构造期已保证
            try:
                raw_items = await self._battle_auth.pop_commands_if_active(
                    match_id, ident, queue, count
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                return gmpb.PollCommandsResponse(code=_to_proto_code(exc))
        else:
            code = self._check(
                context, _ds_scope(match_id=match_id, require_token=True)
            )
            if code != 0:
                return gmpb.PollCommandsResponse(code=code)
            try:
                raw_items = await self._rdb.rpop(queue, count)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                return gmpb.PollCommandsResponse(code=_to_proto_code(exc))

        commands = []
        for raw in raw_items or ():
            cmd = gmpb.GmCommand()
            try:
                cmd.ParseFromString(raw if isinstance(raw, bytes) else bytes(raw))
            except Exception as exc:  # noqa: BLE001 —— 队列里混进了非本协议的字节
                # 跳过而不是整批失败:一条坏记录不该让这台 DS 再也拿不到任何指令。
                # 它已经被 RPOP 掉了,不会反复刷屏。
                plog.get().warning(
                    EVENT_UNMARSHAL_FAILED, match_id=match_id, err=str(exc)
                )
                continue
            commands.append(cmd)
        if commands:
            plog.get().info(
                EVENT_DELIVERED,
                match_id=match_id,
                count=len(commands),
                ds_pod=request.ds_pod_name,
            )
        return gmpb.PollCommandsResponse(code=commonpb.OK, commands=commands)

    # ── RPC 3:AckCommand ─────────────────────────────────────────────────

    async def AckCommand(self, request, context):  # noqa: N802, ANN001, C901, PLR0911
        """战斗 DS 回报执行结果。"""
        match_id = request.match_id
        if match_id == 0:
            return gmpb.AckCommandResponse(code=commonpb.ERR_INVALID_ARG)
        idempotency_key = request.idempotency_key
        if (
            idempotency_key == ""
            or len(idempotency_key.encode("utf-8")) > MAX_ACK_IDEMPOTENCY_KEY_BYTES
        ):
            # 上限必须按**字节**判:Go 的 len(string) 就是字节数,按字符判会让一个
            # 64 个汉字的 key(192 字节)通过,进而撑爆 Redis key 长度预期。
            return gmpb.AckCommandResponse(code=commonpb.ERR_INVALID_ARG)

        if self._model_b:
            ident, code = self._model_b_credential(context, match_id, "")
            if code != 0:
                return gmpb.AckCommandResponse(code=code)
            assert self._battle_auth is not None
            try:
                await self._battle_auth.check_active(match_id, ident)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                return gmpb.AckCommandResponse(code=_to_proto_code(exc))
        else:
            code = self._check(
                context, _ds_scope(match_id=match_id, require_token=True)
            )
            if code != 0:
                return gmpb.AckCommandResponse(code=code)

        message = bounded_ack_message(request.message)
        payload = json.dumps(
            {"ok": request.ok, "message": message} if message else {"ok": request.ok},
            ensure_ascii=False,
            separators=(",", ":"),
        )
        prefix = execution_ack_key_prefix(match_id, idempotency_key)
        try:
            stored = await self._store_ack(
                keys=[prefix + ":result", prefix + ":signal"],
                args=[
                    EXECUTION_ACK_PENDING_VALUE,
                    payload,
                    int(EXECUTION_ACK_TTL_SEC * 1000),
                ],
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            return gmpb.AckCommandResponse(code=_to_proto_code(exc))

        if stored is None or stored is False:
            # 结果键不存在:调用方压根没请求同步等待(没占位),或占位已过 TTL。
            # 这是正常路径 —— 绝大多数指令都是 fire-and-forget。
            plog.get().info(
                EVENT_ACKED,
                match_id=match_id,
                idempotency_key=idempotency_key,
                ok=request.ok,
                waited=False,
            )
            return gmpb.AckCommandResponse(code=commonpb.OK)
        stored_text = _decode(stored)
        if stored_text == payload:
            level = plog.get().info if request.ok else plog.get().warning
            level(
                EVENT_ACKED,
                match_id=match_id,
                idempotency_key=idempotency_key,
                ok=request.ok,
                message=message,
                waited=True,
            )
            return gmpb.AckCommandResponse(code=commonpb.OK)
        if stored_text == EXECUTION_ACK_PENDING_VALUE:
            # 脚本保证"当前是 pending 就一定写成功并返回 payload"。回到 pending
            # 说明脚本正文与这里的判据漂移了 —— 是代码 bug,不是并发。
            plog.get().error(
                EVENT_ACK_STATE_CORRUPT,
                match_id=match_id,
                idempotency_key=idempotency_key,
                hint="store_execution_ack 脚本返回 pending,脚本正文与调用约定已漂移",
            )
            return gmpb.AckCommandResponse(code=commonpb.ERR_INTERNAL)
        # 已被别人写过终态:同一条指令被两台 DS 执行了(或同一台重复 Ack)。
        plog.get().warning(
            EVENT_ACK_CONFLICT,
            match_id=match_id,
            idempotency_key=idempotency_key,
            existing=stored_text,
            hint="同一条 GM 指令已有终态回执,本次 Ack 未生效",
        )
        return gmpb.AckCommandResponse(code=commonpb.ERR_INVALID_STATE)

    # ── 同步等待执行回执 ─────────────────────────────────────────────────

    async def _wait_for_execution_ack(  # noqa: C901, PLR0911, PLR0912
        self, context, match_id: int, idempotency_key: str  # noqa: ANN001
    ) -> int:
        """有界等待 DS 的执行回执。对应 Go 的 `waitForExecutionAck`。

        ★ 有界是 §9 不变量 19/20 的硬要求:DS 崩了 / 队列被 LTRIM 丢弃 / 指令永远
          不被执行时,这里必须在 15s 内返回一个**明确的**码,而不是让 GM 面板挂死。
          到期后返回 ERR_TIMEOUT 属于"重查权威后 fail-closed",不是"假设成功"
          (CLAUDE.md §16.10 的判别标准)。
        """
        prefix = execution_ack_key_prefix(match_id, idempotency_key)
        result_key = prefix + ":result"
        signal_key = prefix + ":signal"
        # 上限取 min(本地 15s, 调用方剩余 deadline):调用方给的时间更短时,再等下去
        # 也没人收得到应答。
        budget = EXECUTION_ACK_WAIT_TIMEOUT_SEC
        remaining = _time_remaining(context)
        if remaining is not None and remaining < budget:
            budget = remaining
        deadline = time.monotonic() + budget

        try:
            current = await self._rdb.get(result_key)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            plog.get().warning(
                EVENT_WAIT_FAILED,
                match_id=match_id,
                idempotency_key=idempotency_key,
                err=str(exc),
            )
            return commonpb.ERR_INTERNAL
        if current is None:
            # 占位与入队在同一个事务里写,读不到 = 事务没生效或 TTL 已过。
            plog.get().error(
                EVENT_MARKER_MISSING,
                match_id=match_id,
                idempotency_key=idempotency_key,
                hint="入队事务已成功却读不到结果占位;指令可能已入队,回执无法确认",
            )
            return commonpb.ERR_INTERNAL
        current_text = _decode(current)
        if current_text != EXECUTION_ACK_PENDING_VALUE:
            # DS 手快,在我们 GET 之前就 Ack 完了。
            return self._decide_execution_result(
                context, match_id, idempotency_key, current_text
            )

        while True:
            left = deadline - time.monotonic()
            if left <= 0:
                plog.get().warning(
                    EVENT_WAIT_TIMEOUT,
                    match_id=match_id,
                    idempotency_key=idempotency_key,
                    waited_sec=round(budget, 3),
                    hint="DS 未在窗口内回执;指令仍在队列里,可能稍后生效",
                )
                return commonpb.ERR_TIMEOUT
            slice_sec = min(EXECUTION_ACK_BLOCK_SLICE_SEC, left)
            try:
                # BLPOP 按 1s 切片:切片让"本地 deadline"由我们自己判,不依赖 Redis
                # 的超时精度,也让取消能在 1s 内被感知。
                popped = await self._rdb.blpop([signal_key], timeout=slice_sec)
            except asyncio.CancelledError:
                # ★ 原样上抛:handler 已被取消,应答本来也发不出去(见模块头形变 1)。
                raise
            except redis_exceptions.RedisError as exc:
                plog.get().warning(
                    EVENT_WAIT_FAILED,
                    match_id=match_id,
                    idempotency_key=idempotency_key,
                    err=str(exc),
                )
                return commonpb.ERR_INTERNAL
            if popped is None:
                continue  # 本切片没等到,回到 while 重判 deadline
            _, raw = popped
            text = _decode(raw)
            if text == "" or text == EXECUTION_ACK_PENDING_VALUE:
                plog.get().warning(
                    EVENT_SIGNAL_INVALID,
                    match_id=match_id,
                    idempotency_key=idempotency_key,
                    signal=text,
                )
                continue
            return self._decide_execution_result(
                context, match_id, idempotency_key, text
            )

    def _decide_execution_result(
        self,
        context,  # noqa: ANN001
        match_id: int,
        idempotency_key: str,
        raw: str,
    ) -> int:
        """把回执 JSON 翻成 in-band code,失败原文经二进制 trailer 回传。"""
        try:
            parsed = json.loads(raw)
            ok = bool(parsed["ok"])
            message = str(parsed.get("message") or "")
        except (ValueError, KeyError, TypeError) as exc:
            plog.get().error(
                EVENT_RESULT_CORRUPT,
                match_id=match_id,
                idempotency_key=idempotency_key,
                raw=raw[:200],
                err=str(exc),
            )
            return commonpb.ERR_INTERNAL
        if ok:
            plog.get().info(
                EVENT_EXECUTION_CONFIRMED,
                match_id=match_id,
                idempotency_key=idempotency_key,
            )
            return commonpb.OK
        plog.get().warning(
            EVENT_EXECUTION_FAILED,
            match_id=match_id,
            idempotency_key=idempotency_key,
            message=message,
        )
        if message:
            # 失败原文走 trailer 而不是塞进 in-band code:code 是给程序判的枚举,
            # 原文是给人看的。`-bin` 后缀的值在 Python grpc 里必须是 bytes(形变 3)。
            try:
                context.set_trailing_metadata(
                    (
                        (
                            EXECUTION_ACK_MESSAGE_METADATA_KEY,
                            message.encode("utf-8"),
                        ),
                    )
                )
            except Exception as exc:  # noqa: BLE001 —— 测试桩 / 已发送 trailer
                plog.get().warning(
                    EVENT_WAIT_FAILED,
                    match_id=match_id,
                    idempotency_key=idempotency_key,
                    err=f"set trailer failed: {exc}",
                )
        return commonpb.ERR_INTERNAL

    # ── 守卫薄封装 ───────────────────────────────────────────────────────

    def _check(self, context, scope) -> int:  # noqa: ANN001
        """`guard.check` 的 None 安全封装(None guard 等价 mode=off)。"""
        if self._ds_guard is None:
            return 0
        return self._ds_guard.check(context, scope)

    def _model_b_credential(
        self, context, match_id: int, pod_name: str  # noqa: ANN001
    ) -> tuple[BattleCredentialIdentity, int]:
        """Model B 要求**完整**凭据身份。对应 Go 的 `modelBCredential`。

        ★ fail-closed 到底:守卫未注入 / 凭据不完整 / 过期时刻缺失,一律
          `ErrUnauthorized`,不给任何"先放行再说"的口子 —— 这条路径能改玩家背包。
        """
        if self._ds_guard is None:
            return BattleCredentialIdentity(), errcode.ErrUnauthorized
        _, verified, code = self._ds_guard.check_credential(
            context, _ds_scope(match_id=match_id, pod=pod_name, require_token=True)
        )
        if code != 0:
            return BattleCredentialIdentity(), code
        if verified is None or verified.exp_ms <= 0:
            return BattleCredentialIdentity(), errcode.ErrUnauthorized
        return (
            BattleCredentialIdentity(
                pod_name=verified.pod,
                instance_uid=verified.instance_uid,
                instance_epoch=verified.protocol_epoch,
                gen=verified.gen,
                jti=verified.jti,
                exp_ms=verified.exp_ms,
                kid=verified.kid,
                token_sha256=verified.token_sha256,
                writer_epoch=verified.writer_epoch,
            ),
            0,
        )


def _ds_scope(*, match_id: int, pod: str = "", require_token: bool = False):  # noqa: ANN202
    """构造 `dsauth.DSScope`(延迟 import,理由同 service.py)。"""
    from pandorapy import dsauth  # noqa: PLC0415

    return dsauth.DSScope(
        ds_type="battle", match_id=match_id, pod=pod, require_token=require_token
    )


def _time_remaining(context) -> float | None:  # noqa: ANN001
    """调用方剩余 deadline(秒);无 deadline 或测试桩返回 None。"""
    getter = getattr(context, "time_remaining", None)
    if getter is None:
        return None
    try:
        remaining = getter()
    except Exception:  # noqa: BLE001 —— 测试桩
        return None
    if remaining is None:
        return None
    return float(remaining)


__all__ = [
    "DEFAULT_POLL_MAX",
    "EXECUTION_ACK_PENDING_VALUE",
    "EXECUTION_ACK_TTL_SEC",
    "EXECUTION_ACK_WAIT_TIMEOUT_SEC",
    "MAX_ACK_IDEMPOTENCY_KEY_BYTES",
    "MAX_ACK_MESSAGE_BYTES",
    "MAX_BAG_TYPE",
    "MAX_POLL_MAX",
    "MAX_QUEUE_LEN",
    "QUEUE_TTL_SEC",
    "STORE_EXECUTION_ACK_SCRIPT",
    "WAIT_EXECUTION_ACK_METADATA_KEY",
    "WAIT_EXECUTION_ACK_METADATA_VALUE",
    "BattleAuthRepo",
    "BattleCredentialGuard",
    "BattleLivenessChecker",
    "GmService",
    "bounded_ack_message",
    "execution_ack_key_prefix",
    "queue_key",
]
