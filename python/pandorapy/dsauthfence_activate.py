"""DS-auth 激活/推进控制面 —— 对应 Go 侧 `pkg/dsauthfence/activate.go`(1273 行)。

★ 这个文件只服务**审计工具与推进工具**。业务进程一律不得调用这里的推进 API
  (Go 侧注释原话:「ActivationClient 只供审计/推进工具使用;业务进程不得调用推进 API」)。
  业务进程走的是 `dsauthfence.acquire_runtime` —— 那条链在**另一个文件**里,本模块
  刻意不提供任何"顺手也能注册 capability"的入口。

★ 与 Go 栈共读同一份 etcd(strangler 模式)。因此下面三类东西**逐字节照抄**,
  写错一个字符就等于两栈各写各的 key,fencing 形同虚设:

    ① key 模板     required-writer-epoch / capabilities/ / activation-lock /
                   activations/<epoch> / activations/policies/<gen>@<policy>
                   / <prefix 去尾斜杠>-genesis-continuity
       —— 全部来自 `dsauthfence` 的同一份构造函数,本文件**不自己拼**。
    ② activation record 的 JSON 字段名与字段顺序(Go `json.Marshal` 按结构体字段序
       输出,omitempty 跳过零值)。Go 写、Python 读,或反过来,都必须解得开。
    ③ audit findings 的文案。运维照着 Go 的输出建了检索,文案漂移 = 检索失效。

★ 本模块**不**实现 fence / etcd 客户端 / 安全配置 —— 那三块是
  `pandorapy.dsauthfence` 的权威。见文件末尾「依赖契约」块。

Python 侧相对 Go 的三处**必须补**的差异(不是风格选择):

  1. aetcd 没有自动 KeepAlive。Go 的 `clientv3.KeepAlive` 是一条流,租约没了流就断;
     aetcd 只有一次性的 `lease.refresh()`,而 etcd 对**已经不存在**的 lease 的
     KeepAlive 应答是「正常返回 + TTL=0」。所以激活锁的续约必须由本模块驱动,
     并且失租时**立刻自 fencing**(见 `ActivationLock._declare_lost`):
     此后本进程的任何推进 API 一律本地拒绝,不再发出 CAS。
     只打日志不自 fencing 是不够的 —— 那样进程还会继续按"我还持锁"的假设往下走。
  2. 本地安全截止线一律 `time.monotonic()`。用 `time.time()` 的话,一次 NTP 回拨
     就能把"我还在安全窗内"这个判断变成永真。
  3. 所有 etcd 调用都套 `asyncio.wait_for` 的**有界超时**(§9 不变量 19/20)。
     Go 侧靠调用方传 ctx deadline;Python 侧不能指望调用方,超时在本模块内闭合。

关于「续约不确定按失败处理」:`refresh_or_raise` 抛连接层异常时只说明**没能证明**
自己还持有,这时按本地安全窗重试是对的(etcd 短抖动很常见);但一旦越过安全线,
或服务端明确回 TTL<=0,都必须立刻当作失主 —— 绝不乐观当成功。
"""

from __future__ import annotations

import asyncio
import binascii
import contextlib
import json
import secrets
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from pandorapy import dsauthfence
from pandorapy import log as plog
from pandorapy import safego
from pandorapy.errcode import (
    ErrInvalidArg,
    ErrInvalidState,
    ErrUnavailable,
    PandoraError,
)
from pandorapy.etcdlease import LeaseGoneError, refresh_or_raise

# ── 整型边界(硬性要求 7)────────────────────────────────────────────────────
#
# Go 的 uint32 / int64 在越界时**编译期或解码期**就炸;Python 的 int 无限精度,
# 一个从 JSON / 命令行进来的 2**64 会一路飘到 etcd compare 里,变成"CAS 永远不成立"
# 这种查不出原因的失败。所以边界在本模块显式判。
_U32_MAX = (1 << 32) - 1
_I64_MAX = (1 << 63) - 1
_I64_MIN = -(1 << 63)

# 激活锁默认 TTL(秒)。与 Go 侧 AcquireLock 的 `if ttl <= 0 { ttl = 30 }` 同值。
_DEFAULT_LOCK_TTL_SEC = 30

# 续约节奏:每 TTL/3 续一次,本地安全线比服务端到期提前 TTL/3。
# 与 pandorapy.snowflake_etcd / etcdleader 同口径 —— 三处若不一致,排障时没人能
# 回答"这个副本到底还该不该认为自己持有"。
_KEEPALIVE_DIVISOR = 3
_SAFETY_MARGIN_DIVISOR = 3


# ── 依赖解析(dsauthfence 尚在并行移植中,见文件末尾契约)────────────────────
def _dep(*names: str) -> Any:
    """按顺序取 `dsauthfence` 的公开面;全都没有就 fail-closed 报清楚缺哪个。

    ★ 刻意**不**在这里补一份实现。fence / etcd key 模板只能有一份权威:
      补一份的后果是两栈(甚至同栈两个模块)对同一个 key 有两种拼法,
      而这种分歧在测试里通常照样绿 —— 因为两边都在用自己那份。
    """
    for name in names:
        value = getattr(dsauthfence, name, None)
        if value is not None:
            return value
    raise PandoraError(
        ErrUnavailable,
        "dsauthfence 缺少必需符号 %s(见 dsauthfence_activate 的依赖契约块)" % (" / ".join(names),),
    )


def _err_of(fn: Callable[..., Any], *args: Any) -> BaseException | None:
    """把「Go 的 error 返回」翻译成 Python 的可选异常。

    同时兼容两种下游写法:抛异常(Python 惯例)与返回异常对象(直译 Go)。
    返回 None 表示校验通过。
    """
    try:
        rv = fn(*args)
    except asyncio.CancelledError:
        raise
    except BaseException as exc:  # noqa: BLE001 —— 这里就是要把校验失败收成值
        return exc
    if isinstance(rv, BaseException):
        return rv
    return None


def _err_text(exc: BaseException) -> str:
    """取与 Go `err.Error()` 等价的文案。

    `PandoraError.__str__` 会带上 `errcode=NN ` 前缀 —— 那个前缀进了 findings 就
    与 Go 的输出不再逐字节一致,所以这里只取 msg。
    """
    if isinstance(exc, PandoraError):
        return exc.msg
    return str(exc)


def _b(value: str) -> bytes:
    return value.encode("utf-8")


def _s(value: bytes | str) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", "surrogateescape")
    return value


def _go_quote(value: str) -> str:
    """Go 的 `%q`(双引号 + Go 语法转义)。

    findings 文案里有 5 处用 `%q`。Python 的 `repr()` 用单引号、转义规则也不同,
    直接用会让文案与 Go 不一致 —— 这正是硬性要求 5 要防的那种漂移。
    Go 对**可打印**的非 ASCII 字符保持原样,只转义引号 / 反斜杠 / 控制字符。
    """
    out = ['"']
    for ch in value:
        if ch == '"':
            out.append('\\"')
        elif ch == "\\":
            out.append("\\\\")
        elif ch == "\n":
            out.append("\\n")
        elif ch == "\t":
            out.append("\\t")
        elif ch == "\r":
            out.append("\\r")
        elif ch < " " or ch == "\x7f":
            out.append("\\x%02x" % ord(ch))
        else:
            out.append(ch)
    out.append('"')
    return "".join(out)


def _require_u32(value: int, what: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not (0 <= value <= _U32_MAX):
        raise PandoraError(ErrInvalidArg, "%s must fit in uint32" % (what,))
    return value


def _require_i64(value: int, what: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not (_I64_MIN <= value <= _I64_MAX):
        raise PandoraError(ErrInvalidArg, "%s must fit in int64" % (what,))
    return value


def _now_unix_milli() -> int:
    """墙钟毫秒 —— 对应 Go 的 `time.Now().UnixMilli()`。

    ★ 这里用墙钟是**对的**:它写进 activation record 供跨机器审计比对,必须是绝对
      时间。与之相对,判定"锁还在不在我手上"的本地安全线一律用 `time.monotonic()`
      (见 `ActivationLock`),两者不能互换。
    """
    return int(time.time() * 1000)


# ── 数据结构 ────────────────────────────────────────────────────────────────
@dataclass(frozen=True, slots=True)
class LiveCapability:
    """capability 与 etcd lease 元数据的组合。对应 Go 的 LiveCapability。"""

    capability: Any
    lease_id: int
    key: str
    mod_revision: int


@dataclass(frozen=True, slots=True)
class RequiredSnapshot:
    """required writer 的一次线性读结果。

    Value 相同不足以做推进 CAS:key 曾 1→2→1 时只有 ModRevision 能识别 ABA。
    """

    epoch: int = 0
    policy_generation: int = 0
    policy_id: str = ""
    raw_value: str = ""
    mod_revision: int = 0


@dataclass(slots=True)
class AuditPolicy:
    """推进前 capability 快照必须满足的精确条件。对应 Go 的 AuditPolicy。"""

    prefix: str = ""
    required_services: Mapping[str, int] = field(default_factory=dict)
    required_instances: Mapping[str, set[str]] = field(default_factory=dict)
    target_epoch: int = 0
    target_policy_generation: int = 0
    expected_acquired_policy_generation: int = 0
    keyset_revision: str = ""
    etcd_identity_revision: str = ""
    allowed_digests: set[str] = field(default_factory=set)
    expected_digests: Mapping[str, str] = field(default_factory=dict)
    required_features: Mapping[str, set[str]] = field(default_factory=dict)


# activation record 的字段顺序 = Go 结构体字段顺序。json.Marshal 按声明序输出,
# 跨栈读写要想拿到**逐字节相同**的审计载荷,这个顺序不能动。
# 第三元素是 omitempty:零值时不输出该键。
_RECORD_FIELDS: tuple[tuple[str, str, bool], ...] = (
    ("from_", "from", False),
    ("to", "to", False),
    ("from_required_value", "from_required_value", False),
    ("to_required_value", "to_required_value", False),
    ("required_policy_id", "required_policy_id", False),
    ("from_policy_generation", "from_policy_generation", True),
    ("to_policy_generation", "to_policy_generation", True),
    ("from_mod_revision", "from_mod_revision", False),
    ("expected_services_hash", "expected_services_hash", False),
    ("activation_evidence_sha256", "activation_evidence_sha256", False),
    ("activation_evidence_completed_at_ms", "activation_evidence_completed_at_ms", False),
    ("activated_at_ms", "activated_at_ms", False),
    ("zero_writer_bootstrap", "zero_writer_bootstrap", True),
    ("genesis_bootstrap", "genesis_bootstrap", True),
)

# 每个 JSON 键的 Go 静态类型。Python 的 json 会把 `"from": -1` 或 `"from": "1"`
# 悄悄收下,而 Go 的 json.Unmarshal 会直接报错 —— 少了这张表,一条被篡改成
# 负数 epoch 的审计记录能在 Python 侧通过解码。
_RECORD_TYPES: Mapping[str, str] = {
    "from": "u32",
    "to": "u32",
    "from_required_value": "str",
    "to_required_value": "str",
    "required_policy_id": "str",
    "from_policy_generation": "u32",
    "to_policy_generation": "u32",
    "from_mod_revision": "i64",
    "expected_services_hash": "str",
    "activation_evidence_sha256": "str",
    "activation_evidence_completed_at_ms": "i64",
    "activated_at_ms": "i64",
    "zero_writer_bootstrap": "bool",
    "genesis_bootstrap": "bool",
}


@dataclass(slots=True)
class ActivationRecord:
    """与 required_writer_epoch 推进**同一个 etcd 事务**写入的不可变审计记录。

    `activation_evidence_sha256` 把外部 create-only 的 K8s 证据标记(含已完成的
    preflight Job/Pod 与精确配置身份)绑到这次 epoch 迁移上:激活之后再创建的
    同名 Job 因此永远满足不了 epoch-2 的审计。
    """

    from_: int = 0
    to: int = 0
    from_required_value: str = ""
    to_required_value: str = ""
    required_policy_id: str = ""
    from_policy_generation: int = 0
    to_policy_generation: int = 0
    from_mod_revision: int = 0
    expected_services_hash: str = ""
    activation_evidence_sha256: str = ""
    activation_evidence_completed_at_ms: int = 0
    activated_at_ms: int = 0
    zero_writer_bootstrap: bool = False
    genesis_bootstrap: bool = False

    def to_json_bytes(self) -> bytes:
        payload: dict[str, Any] = {}
        for attr, key, omitempty in _RECORD_FIELDS:
            value = getattr(self, attr)
            if omitempty and not value:
                continue
            payload[key] = value
        # 分隔符与 Go 的 json.Marshal 一致(无空格);中文不会出现在本记录里,
        # ensure_ascii 保持 False 以免与 Go 的 UTF-8 直出产生差异。
        return json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _decode_activation_record(payload: bytes | str) -> ActivationRecord:
    """对应 Go 的 decodeActivationRecord(DisallowUnknownFields + 拒绝尾随 JSON)。

    ★ 这里刻意比 `json.loads` 严:未知键、类型不符、越界整数、尾随内容一律拒。
      审计记录是"事后唯一能证明这次推进合法"的东西,宽松解码等于允许伪造。
      Go 的 Unmarshal 还有个大小写不敏感的匹配回退(`"From"` 也能命中 `from` tag),
      本移植**不**复刻那条回退:两栈的写入侧都产出精确小写键,收紧不会误拒真记录,
      却能挡掉一类靠大小写变体绕过审查的构造。
    """
    text = payload.decode("utf-8") if isinstance(payload, bytes) else payload
    decoder = json.JSONDecoder()
    try:
        obj, end = decoder.raw_decode(text)
    except ValueError as exc:
        raise PandoraError(ErrInvalidArg, "invalid activation record json", cause=exc) from exc
    if text[end:].strip():
        raise PandoraError(ErrInvalidArg, "activation record contains trailing JSON")
    if not isinstance(obj, dict):
        raise PandoraError(ErrInvalidArg, "activation record must be a JSON object")

    record = ActivationRecord()
    attr_by_key = {key: attr for attr, key, _ in _RECORD_FIELDS}
    for key, value in obj.items():
        attr = attr_by_key.get(key)
        if attr is None:
            raise PandoraError(ErrInvalidArg, "unknown activation record field %s" % (key,))
        kind = _RECORD_TYPES[key]
        if kind == "str":
            if not isinstance(value, str):
                raise PandoraError(ErrInvalidArg, "activation record field %s must be string" % (key,))
            setattr(record, attr, value)
        elif kind == "bool":
            if not isinstance(value, bool):
                raise PandoraError(ErrInvalidArg, "activation record field %s must be bool" % (key,))
            setattr(record, attr, value)
        elif kind == "u32":
            if isinstance(value, bool) or not isinstance(value, int) or not (0 <= value <= _U32_MAX):
                raise PandoraError(ErrInvalidArg, "activation record field %s must be uint32" % (key,))
            setattr(record, attr, value)
        else:  # i64
            if isinstance(value, bool) or not isinstance(value, int) or not (_I64_MIN <= value <= _I64_MAX):
                raise PandoraError(ErrInvalidArg, "activation record field %s must be int64" % (key,))
            setattr(record, attr, value)
    return record


# ── etcd 读写小工具 ─────────────────────────────────────────────────────────
@dataclass(frozen=True, slots=True)
class _KV:
    key: str
    value: str
    create_revision: int
    mod_revision: int
    version: int
    lease: int


def _kv_of(raw: Any) -> _KV:
    return _KV(
        key=_s(raw.key),
        value=_s(raw.value),
        create_revision=int(raw.create_revision),
        mod_revision=int(raw.mod_revision),
        version=int(raw.version),
        lease=int(raw.lease),
    )


def _txn_range(responses: Sequence[Any], index: int) -> list[_KV] | None:
    """取事务里第 index 个 Range 应答;不是 Range(或缺应答)返回 None。

    对应 Go 的 `resp.Responses[i].GetResponseRange()` 返回 nil 的判定 ——
    Go 侧每一处都显式判了 nil,这里不能省:少判一处就把"读缺失"当成了"读到空"。
    """
    if index >= len(responses):
        return None
    item = responses[index]
    if not isinstance(item, list):
        return None
    return [_kv_of(kv) for _, kv in item]


def _prefix_range_end(prefix: bytes) -> bytes:
    """对应 clientv3.GetPrefixRangeEnd。"""
    buf = bytearray(prefix)
    for i in reversed(range(len(buf))):
        if buf[i] < 0xFF:
            buf[i] += 1
            return bytes(buf[: i + 1])
    return b"\0"


# ── 激活客户端 ──────────────────────────────────────────────────────────────
class ActivationClient:
    """只操作 DS auth 命名空间的客户端。对应 Go 的 ActivationClient。

    构造走 `new_activation_client*`;直接传 `cli` 是给测试与已持有连接的调用方用的。
    """

    __slots__ = ("_cli", "_prefix", "_timeout", "_owns_client")

    def __init__(self, cli: Any, prefix: str, timeout: float, *, owns_client: bool = True) -> None:
        self._cli = cli
        self._prefix = prefix
        self._timeout = timeout
        self._owns_client = owns_client

    @property
    def prefix(self) -> str:
        return self._prefix

    async def close(self) -> None:
        if not self._owns_client:
            return
        close = getattr(self._cli, "close", None)
        if close is None:
            return
        result = close()
        if asyncio.iscoroutine(result):
            await result

    # ── 有界超时包装 ────────────────────────────────────────────────────
    async def _get(self, key: str) -> _KV | None:
        raw = await asyncio.wait_for(self._cli.get(_b(key)), timeout=self._timeout)
        return None if raw is None else _kv_of(raw)

    async def _get_prefix(self, prefix: str) -> list[_KV]:
        rng = await asyncio.wait_for(self._cli.get_prefix(_b(prefix)), timeout=self._timeout)
        return [_kv_of(kv) for kv in rng]

    async def _txn(
        self, compare: Sequence[Any], success: Sequence[Any]
    ) -> tuple[bool, list[Any]]:
        succeeded, responses = await asyncio.wait_for(
            self._cli.transaction(compare=list(compare), success=list(success), failure=[]),
            timeout=self._timeout,
        )
        return bool(succeeded), list(responses)

    @property
    def _t(self) -> Any:
        return self._cli.transactions

    # ── 激活锁 ──────────────────────────────────────────────────────────
    async def acquire_lock(self, ttl: int = 0) -> ActivationLock:
        """以 lease + create-only CAS 获取激活锁。

        业务 capability 注册事务会同时断言锁不存在 —— 所以这把锁在被持有期间,
        capability 集合是冻结的,audit→CAS 之间的"检查后使用"竞态被封住。
        """
        if ttl <= 0:
            ttl = _DEFAULT_LOCK_TTL_SEC
        _require_i64(ttl, "activation lock ttl")
        lease = await asyncio.wait_for(self._cli.lease(ttl), timeout=self._timeout)
        token = secrets.token_hex(16)
        lock_key = _b(_dep("activation_lock_key")(self._prefix))
        try:
            succeeded, _ = await self._txn(
                [self._t.create(lock_key) == 0],
                [self._t.put(lock_key, _b(token), lease=lease.id)],
            )
        except BaseException:
            await self._revoke_quietly(lease.id)
            raise
        if not succeeded:
            await self._revoke_quietly(lease.id)
            raise PandoraError(ErrInvalidState, "activation lock is held")
        lock = ActivationLock(client=self, lease=lease, token=token, ttl_sec=ttl)
        lock._start_keepalive()
        return lock

    async def _revoke_quietly(self, lease_id: int) -> None:
        """best-effort 释放**自己这一把** lease。

        ★ 精确幂等(释放路径红线):只 revoke 自己 grant 出来的 lease id。
          绝不按 key 删 —— 若本进程已失租、锁已被继任者以新 lease 拿走,
          按 key 删就会把继任者的锁删掉,直接造出双持有者。
          revoke 一个已过期的 lease 在服务端是 no-op,不会误伤任何人。
        """
        with contextlib.suppress(Exception):
            await asyncio.wait_for(self._cli.revoke_lease(lease_id), timeout=self._timeout)

    # ── required 读 / bootstrap ─────────────────────────────────────────
    async def bootstrap_required(self, epoch: int) -> None:
        """仅允许在 key 不存在时创建初始 epoch;不会覆盖或回退。"""
        _require_u32(epoch, "bootstrap epoch")
        if epoch != 1:
            raise PandoraError(
                ErrInvalidArg,
                "bootstrap epoch must be immutable baseline 1, got %d" % (epoch,),
            )
        key = _b(_dep("required_key")(self._prefix))
        value = _dep("required_value_for_epoch")(epoch)
        succeeded, _ = await self._txn(
            [self._t.create(key) == 0],
            [self._t.put(key, _b(value))],
        )
        if not succeeded:
            raise PandoraError(ErrInvalidState, "required epoch already exists; bootstrap refused")

    async def required(self) -> int:
        """线性读取当前 required epoch。"""
        return (await self.required_snapshot()).epoch

    async def required_snapshot(self) -> RequiredSnapshot:
        """线性读取 required 的值与**同一次读取**观察到的 ModRevision。"""
        kv = await self._get(_dep("required_key")(self._prefix))
        if kv is None:
            raise PandoraError(ErrInvalidState, "required epoch missing")
        state = _dep("parse_required_state")(_b(kv.value))
        if kv.mod_revision <= 0:
            raise PandoraError(ErrInvalidState, "required epoch has invalid mod revision")
        return RequiredSnapshot(
            epoch=int(state.epoch),
            policy_generation=int(state.policy_generation),
            policy_id=str(state.policy_id),
            raw_value=str(state.raw_value),
            mod_revision=kv.mod_revision,
        )

    async def capabilities(self) -> list[LiveCapability]:
        """列出仍有 lease 的实时能力;坏记录使审计失败,**绝不跳过**。"""
        kvs = await self._get_prefix(_dep("capability_prefix")(self._prefix))
        out: list[LiveCapability] = []
        for kv in kvs:
            try:
                capability = _capability_from_json(_b(kv.value))
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                raise PandoraError(
                    ErrInvalidArg, "decode capability %s: %s" % (kv.key, _err_text(exc)), cause=exc
                ) from exc
            if kv.lease == 0:
                raise PandoraError(ErrInvalidState, "capability %s has no lease" % (kv.key,))
            out.append(
                LiveCapability(
                    capability=capability,
                    lease_id=kv.lease,
                    key=kv.key,
                    mod_revision=kv.mod_revision,
                )
            )
        # Go 的 sort.Slice 按字符串**字节序**比较;按 UTF-8 编码排序才逐条对齐。
        out.sort(key=lambda live: live.key.encode("utf-8"))
        return out

    # ── genesis continuity ──────────────────────────────────────────────
    async def prepare_missing_required_policy_v3_continuity(self, token: str) -> None:
        """在允许 K8s 把标记推进到 pending 之前,先在 etcd 数据卷上建哨兵。

        这个创建是**一个事务**,同时证明 required / record / capabilities /
        activation-lock 仍然全部不存在。重入只接受精确的不可变哨兵,
        且要求 required / record / capabilities 仍然缺席。
        """
        validate_genesis_continuity_token(token)
        continuity_key = _genesis_continuity_key(self._prefix)
        compares = self._build_missing_zero_writer_continuity_prepare_compares(continuity_key)
        succeeded, _ = await self._txn(
            compares, [self._t.put(_b(continuity_key), _b(token))]
        )
        if succeeded:
            return
        # CAS 没成 ≠ 失败:可能是本次 prepare 的**重入**(上一次已经写下了同一个哨兵)。
        # 重入只有在哨兵是精确的 create-only token、且权威前缀仍然为空时才算通过。
        try:
            await self._verify_missing_required_policy_v3_continuity(token)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            raise PandoraError(
                ErrInvalidState,
                "genesis continuity prepare CAS failed: %s" % (_err_text(exc),),
                cause=exc,
            ) from exc

    async def verify_genesis_continuity(self, token: str) -> None:
        """证明精确的 create-only 哨兵仍在 etcd 数据卷上。

        pending/complete 的 K8s 标记**绝不能**重建一个缺失的哨兵:缺席即数据连续性已断。
        """
        validate_genesis_continuity_token(token)
        kv = await self._get(_genesis_continuity_key(self._prefix))
        if kv is None:
            raise PandoraError(ErrInvalidState, "genesis continuity sentinel missing")
        _require_exact_create_only_sentinel(kv, token)

    async def _verify_missing_required_policy_v3_continuity(self, token: str) -> None:
        authority_prefix = _dep("clean_prefix")(self._prefix)
        # Go 侧这次 range 带了 WithLimit(1) —— 纯性能优化。aetcd 的事务 Get
        # 不支持 limit,这里读全量后判空,判定语义完全相同。
        _, responses = await self._txn(
            [],
            [
                self._t.get(_b(_genesis_continuity_key(self._prefix))),
                self._t.get(_b(authority_prefix), _prefix_range_end(_b(authority_prefix))),
            ],
        )
        if len(responses) != 2:
            raise PandoraError(ErrInvalidState, "genesis continuity verification read is incomplete")
        sentinel = _txn_range(responses, 0)
        authority = _txn_range(responses, 1)
        if sentinel is None or len(sentinel) != 1:
            raise PandoraError(ErrInvalidState, "genesis continuity sentinel missing")
        _require_exact_create_only_sentinel(sentinel[0], token)
        if authority is None or len(authority) != 0:
            raise PandoraError(
                ErrInvalidState,
                "DS-auth authority prefix is not empty after genesis continuity prepare",
            )

    def _build_missing_zero_writer_continuity_prepare_compares(self, continuity_key: str) -> list[Any]:
        authority_prefix = _b(_dep("clean_prefix")(self._prefix))
        return [
            self._t.create(_b(continuity_key)) == 0,
            self._t.create(authority_prefix, _prefix_range_end(authority_prefix)) == 0,
        ]

    # ── V3 证据校验(只读)───────────────────────────────────────────────
    async def verify_required_policy_v3_activation_evidence(
        self, activation_evidence_sha256: str, activation_evidence_completed_at_ms: int
    ) -> None:
        await self._verify_required_policy_v3_activation_evidence(
            activation_evidence_sha256, activation_evidence_completed_at_ms, ""
        )

    async def verify_required_policy_v3_activation_evidence_and_continuity(
        self,
        activation_evidence_sha256: str,
        activation_evidence_completed_at_ms: int,
        genesis_continuity_token: str,
    ) -> None:
        """在**同一个** etcd 事务里读 required、不可变激活记录与精确数据卷哨兵。

        本地 pending/complete 标记走这条而不是分开三次读 —— 分开读的话,
        一次数据卷替换可以藏在两次读之间。
        """
        validate_genesis_continuity_token(genesis_continuity_token)
        await self._verify_required_policy_v3_activation_evidence(
            activation_evidence_sha256,
            activation_evidence_completed_at_ms,
            genesis_continuity_token,
        )

    async def _verify_required_policy_v3_activation_evidence(
        self,
        activation_evidence_sha256: str,
        activation_evidence_completed_at_ms: int,
        genesis_continuity_token: str,
    ) -> None:
        _validate_activation_evidence_sha256(activation_evidence_sha256)
        _require_i64(activation_evidence_completed_at_ms, "activation evidence completion time")
        record = await self._verify_required_policy_v3_activation_record_with_continuity(
            genesis_continuity_token
        )
        if (
            record.activation_evidence_sha256 != activation_evidence_sha256
            or record.activation_evidence_completed_at_ms != activation_evidence_completed_at_ms
        ):
            raise PandoraError(ErrInvalidState, "V3 policy activation record evidence mismatch")

    async def verify_required_policy_v3_activation_record(self) -> None:
        """只读的启动 / 恢复期证明。

        校验 required V3 与规范的 create-only 激活记录是在**同一个事务**里写下的,
        含 genesis / V1 路径的 zero-writer 来源。生产迁移会另外调上面带精确证据的
        变体,把外部不可变 staging 标记也绑上。
        """
        await self._verify_required_policy_v3_activation_record_with_continuity("")

    async def _verify_required_policy_v3_activation_record_with_continuity(
        self, genesis_continuity_token: str
    ) -> ActivationRecord:
        deps = _Deps()
        required_epoch_key = deps.required_key(self._prefix)
        record_key = _policy_activation_record_key(
            self._prefix, deps.policy_generation_v3, deps.policy_v3
        )
        ops = [self._t.get(_b(required_epoch_key)), self._t.get(_b(record_key))]
        if genesis_continuity_token != "":
            validate_genesis_continuity_token(genesis_continuity_token)
            ops.append(self._t.get(_b(_genesis_continuity_key(self._prefix))))
        _, responses = await self._txn([], ops)
        if len(responses) != len(ops):
            raise PandoraError(ErrInvalidState, "V3 policy activation evidence read is incomplete")
        required_kvs = _txn_range(responses, 0)
        record_kvs = _txn_range(responses, 1)
        if (
            required_kvs is None
            or len(required_kvs) != 1
            or required_kvs[0].value != deps.required_value_v3
            or record_kvs is None
            or len(record_kvs) != 1
        ):
            raise PandoraError(
                ErrInvalidState, "required V3 policy or immutable activation record missing"
            )
        required_kv, record_kv = required_kvs[0], record_kvs[0]

        continuity_create_revision = 0
        if genesis_continuity_token != "":
            continuity_kvs = _txn_range(responses, 2)
            if continuity_kvs is None or len(continuity_kvs) != 1:
                raise PandoraError(
                    ErrInvalidState,
                    "genesis continuity sentinel missing from V3 evidence transaction",
                )
            continuity_kv = continuity_kvs[0]
            _require_exact_create_only_sentinel(continuity_kv, genesis_continuity_token)
            continuity_create_revision = continuity_kv.create_revision

        if (
            record_kv.create_revision <= 0
            or record_kv.version != 1
            or record_kv.mod_revision != record_kv.create_revision
            or required_kv.mod_revision != record_kv.create_revision
        ):
            raise PandoraError(
                ErrInvalidState,
                "V3 policy activation record is not the immutable required-policy transaction",
            )
        try:
            record = _decode_activation_record(record_kv.value)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            raise PandoraError(
                ErrInvalidArg,
                "decode V3 policy activation record: %s" % (_err_text(exc),),
                cause=exc,
            ) from exc

        if genesis_continuity_token != "":
            _validate_genesis_continuity_activation_provenance(
                record, continuity_create_revision, record_kv.create_revision
            )

        valid_from = False
        if record.from_policy_generation == 0:
            valid_from = (
                record.genesis_bootstrap
                and record.zero_writer_bootstrap
                and record.from_ == 0
                and record.from_required_value == ""
                and record.from_mod_revision == 0
            )
        elif record.from_policy_generation in (deps.policy_generation_v1, deps.policy_generation_v2):
            from_value_err = None
            from_writer_err = None
            from_value = ""
            from_writer = 0
            try:
                from_value = deps.required_value_for_policy_generation(record.from_policy_generation)
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                from_value_err = exc
            try:
                from_writer = deps.required_writer_epoch_for_policy_generation(
                    record.from_policy_generation
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                from_writer_err = exc
            valid_from = (
                from_value_err is None
                and from_writer_err is None
                and record.from_ == from_writer
                and record.from_required_value == from_value
                and record.from_mod_revision > 0
                and record.from_mod_revision < record_kv.create_revision
                and not record.genesis_bootstrap
                and (
                    (record.from_policy_generation == deps.policy_generation_v1)
                    == record.zero_writer_bootstrap
                )
            )

        if (
            not valid_from
            or record.to_policy_generation != deps.policy_generation_v3
            or record.to != deps.protocol_epoch_v2
            or record.to_required_value != deps.required_value_v3
            or record.required_policy_id != deps.policy_v3
            or not _is_canonical_lower_hex_sha256(record.expected_services_hash)
            or record.activated_at_ms <= 0
            or record.activation_evidence_completed_at_ms <= 0
            or record.activation_evidence_completed_at_ms > record.activated_at_ms
        ):
            raise PandoraError(ErrInvalidState, "V3 policy activation record is not canonical")

        _validate_zero_writer_services_topology(record)

        if record.genesis_bootstrap and (
            required_kv.version != 1 or required_kv.create_revision != record_kv.create_revision
        ):
            raise PandoraError(
                ErrInvalidState,
                "V3 genesis record is not a create-only empty-services transaction",
            )
        err = _err_of(_validate_activation_evidence_sha256, record.activation_evidence_sha256)
        if err is not None:
            raise PandoraError(
                ErrInvalidState,
                "V3 policy activation record has invalid evidence: %s" % (_err_text(err),),
                cause=err,
            ) from err
        return record

    async def verify_activation_evidence(
        self, target: int, activation_evidence_sha256: str, activation_evidence_completed_at_ms: int
    ) -> None:
        """线性读取不可变的目标 activation record,并要求精确的证据摘要。

        legacy epoch-1 状态没有 activation record,仍然可读;epoch-2 在记录或
        证据字段缺失 / 畸形时**绝不回退**。
        """
        _require_u32(target, "activation evidence target")
        if target <= 1:
            raise PandoraError(
                ErrInvalidArg,
                "activation evidence target must be greater than baseline: %d" % (target,),
            )
        _validate_activation_evidence_sha256(activation_evidence_sha256)
        _require_i64(activation_evidence_completed_at_ms, "activation evidence completion time")
        deps = _Deps()
        required_epoch_key = deps.required_key(self._prefix)
        record_key = deps.clean_prefix(self._prefix) + "activations/" + str(target)
        _, responses = await self._txn(
            [], [self._t.get(_b(required_epoch_key)), self._t.get(_b(record_key))]
        )
        if len(responses) != 2:
            raise PandoraError(
                ErrInvalidState, "activation evidence read for epoch %d is incomplete" % (target,)
            )
        required_kvs = _txn_range(responses, 0)
        record_kvs = _txn_range(responses, 1)
        target_value = deps.required_value_for_epoch(target)
        if required_kvs is None or len(required_kvs) != 1 or required_kvs[0].value != target_value:
            raise PandoraError(
                ErrInvalidState,
                "required epoch %d is not current while verifying activation evidence" % (target,),
            )
        if record_kvs is None or len(record_kvs) != 1:
            raise PandoraError(
                ErrInvalidState, "activation record for epoch %d missing" % (target,)
            )
        required_kv, record_kv = required_kvs[0], record_kvs[0]
        # 两个 key 由同一次激活事务写下。Version=1 额外拒掉对这条名义上不可变记录的
        # 任何后续覆盖 —— 哪怕攻击者把 JSON 字段原样保留。
        if (
            record_kv.create_revision <= 0
            or record_kv.version != 1
            or record_kv.mod_revision != record_kv.create_revision
            or required_kv.mod_revision != record_kv.create_revision
        ):
            raise PandoraError(
                ErrInvalidState,
                "activation record for epoch %d is not the immutable required-epoch transaction"
                % (target,),
            )
        try:
            record = _decode_activation_record(record_kv.value)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            raise PandoraError(
                ErrInvalidArg,
                "decode activation record for epoch %d: %s" % (target, _err_text(exc)),
                cause=exc,
            ) from exc
        if (
            record.from_ == 0
            or record.to != target
            or record.from_ >= record.to
            or record.from_required_value != "1"
            or record.to_required_value != target_value
            or record.required_policy_id != deps.policy_v2
            or record.from_mod_revision <= 0
            or record.from_mod_revision >= record_kv.create_revision
            or not _is_canonical_lower_hex_sha256(record.expected_services_hash)
            or record.activated_at_ms <= 0
            or record.activation_evidence_completed_at_ms <= 0
            or record.activation_evidence_completed_at_ms > record.activated_at_ms
        ):
            raise PandoraError(
                ErrInvalidState, "activation record for epoch %d is not canonical" % (target,)
            )
        err = _err_of(_validate_activation_evidence_sha256, record.activation_evidence_sha256)
        if err is not None:
            raise PandoraError(
                ErrInvalidState,
                "activation record for epoch %d has invalid evidence: %s" % (target, _err_text(err)),
                cause=err,
            ) from err
        if record.activation_evidence_sha256 != activation_evidence_sha256:
            raise PandoraError(
                ErrInvalidState, "activation record evidence mismatch for epoch %d" % (target,)
            )
        if record.activation_evidence_completed_at_ms != activation_evidence_completed_at_ms:
            raise PandoraError(
                ErrInvalidState,
                "activation record evidence completion mismatch for epoch %d" % (target,),
            )


async def new_activation_client(
    endpoints: Sequence[str], prefix: str = "", timeout: float = 0.0
) -> ActivationClient:
    """构造只操作 DS auth 命名空间的客户端。"""
    return await new_activation_client_with_security(endpoints, prefix, timeout, None)


async def new_activation_client_with_security(
    endpoints: Sequence[str], prefix: str = "", timeout: float = 0.0, security: Any = None
) -> ActivationClient:
    """构造带 mTLS/auth/ACL 负向证明的激活客户端。"""
    if not endpoints:
        raise PandoraError(ErrInvalidArg, "dsauthfence: empty endpoints")
    if prefix == "":
        prefix = _dep("DEFAULT_PREFIX")
    if timeout <= 0:
        timeout = float(_dep("DEFAULT_DIAL_TIMEOUT"))
    if security is None:
        security = _dep("ClientSecurity")()
    cli = _dep("new_etcd_client")(list(endpoints), timeout, prefix, security)
    if asyncio.iscoroutine(cli):
        cli = await cli
    return ActivationClient(cli=cli, prefix=prefix, timeout=timeout)


# ── 激活锁 ──────────────────────────────────────────────────────────────────
class ActivationLock:
    """冻结 capability 集合,封住 audit→CAS 的检查使用竞态。

    Python 侧比 Go 多一层**本地自 fencing**:aetcd 没有自动 KeepAlive,续约循环
    由本类驱动;一旦续约被服务端明确否掉(TTL<=0),或本地安全线越过,
    本锁立刻进入 lost 态,之后所有推进 API **在发出 CAS 之前**就本地拒绝。

    为什么不能"反正 etcd 那边 CAS 会失败,本地不用管":
      etcd 侧确实会拒(锁 key 已随 lease 消失,`Value(lockKey)==token` 不成立),
      但那是**发出请求之后**才知道的。期间调用方会照常跑完整个 audit、把结果当
      "已冻结的快照"用,并在失败后倾向于"重试一次" —— 而此时锁可能已在别人手里,
      重试会踩在别人的冻结窗口上。自 fencing 让失租在本地立即变成硬失败。
    """

    __slots__ = (
        "_client",
        "_lease",
        "_lease_id",
        "_token",
        "_ttl_sec",
        "_task",
        "_lost",
        "_lost_reason",
        "_safe_deadline",
        "_topology_lease_verified",
        "_closed",
    )

    def __init__(self, client: ActivationClient, lease: Any, token: str, ttl_sec: int) -> None:
        self._client = client
        self._lease = lease
        self._lease_id = int(lease.id)
        self._token = token
        self._ttl_sec = ttl_sec
        self._task: asyncio.Task | None = None
        self._lost = asyncio.Event()
        self._lost_reason = ""
        # ★ 本地安全线一律 monotonic:墙钟回拨会让"我还在安全窗内"永真。
        self._safe_deadline = time.monotonic() + ttl_sec - ttl_sec / _SAFETY_MARGIN_DIVISOR
        # 在真正的控制面 provider 校验器落地之前,没有任何代码路径会把它置 True。
        # 它挡住"库调用方绕过 release 包装,拿一个自证的 K8s 标记就推进"。
        self._topology_lease_verified = False
        self._closed = False

    @property
    def token(self) -> str:
        return self._token

    @property
    def lost(self) -> asyncio.Event:
        return self._lost

    @property
    def lost_reason(self) -> str:
        return self._lost_reason

    # ── 续约 ────────────────────────────────────────────────────────────
    def _start_keepalive(self) -> None:
        if self._task is not None:
            return
        # 后台循环走 safego 并带名字(硬性要求 4):裸 create_task 抛异常后会
        # 静默躺在 Task 里,循环已死而进程照常服务。
        self._task = safego.spawn("dsauthfence-activation-lock-keepalive", self._keepalive_loop)

    async def _keepalive_loop(self) -> None:
        interval = self._ttl_sec / _KEEPALIVE_DIVISOR
        margin = self._ttl_sec / _SAFETY_MARGIN_DIVISOR
        while True:
            try:
                await asyncio.sleep(
                    min(interval, max(0.05, self._safe_deadline - time.monotonic()))
                )
                started = time.monotonic()
                await refresh_or_raise(self._lease, timeout=interval)
                # ★ 只有成功应答才推进安全线,且从**发起时刻**算(保守方向)。
                self._safe_deadline = started + self._ttl_sec - margin
            except asyncio.CancelledError:
                raise
            except LeaseGoneError as exc:
                # 服务端明确说 lease 不存在 = 锁已经不是我的了。这是**证据**不是抖动,
                # 不走安全窗重试:此刻锁可能已被继任者拿走。
                self._declare_lost("lease_gone", str(exc))
                return
            except BaseException as exc:  # noqa: BLE001
                if time.monotonic() < self._safe_deadline:
                    # 还在安全窗内:etcd 短抖动常见,继续试。
                    # ★ 注意这**不是**"到期后假设成功"(§16.10 禁止项):越线后走的是
                    #   下面的 fail-closed 分支,而不是继续当自己持有。
                    continue
                self._declare_lost("deadline_exceeded", repr(exc))
                return

    def _declare_lost(self, reason: str, err: str) -> None:
        if self._lost.is_set():
            return
        self._lost_reason = reason
        self._lost.set()
        plog.get().error(
            "dsauthfence_activation_lock_lost",
            reason=reason,
            err=err,
            lease_id=self._lease_id,
            hint=(
                "激活锁独占权已不可证明。必须停止一切推进 CAS —— "
                "capability 集合不再冻结,继续推进会踩在继任者的审计窗口上"
            ),
        )

    def _ensure_held(self) -> None:
        """推进 API 的本地闸。失租 / 越过安全线一律 fail-closed。"""
        if self._closed:
            raise PandoraError(ErrInvalidState, "activation lock is closed")
        if self._lost.is_set():
            raise PandoraError(
                ErrInvalidState, "activation lock lease lost: %s" % (self._lost_reason,)
            )
        if time.monotonic() >= self._safe_deadline:
            # 越线 ≠ "假设还持有";这里立刻记为失主并拒绝,是 §16.10 允许的
            # 「到期后 fail-closed」而不是「到期后假设成功」。
            self._declare_lost("deadline_exceeded", "local safety deadline crossed before advance")
            raise PandoraError(
                ErrInvalidState, "activation lock lease lost: %s" % (self._lost_reason,)
            )

    async def close(self) -> None:
        """释放激活锁。幂等。

        ★ 精确幂等:只 revoke **自己 grant 的 lease**,绝不按 key 删。
          本进程若已失租,锁可能已在继任者手里;按 key 删会把继任者的锁删掉。
        """
        self._closed = True
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._task
            self._task = None
        await self._client._revoke_quietly(self._lease_id)

    # ── 推进 ────────────────────────────────────────────────────────────
    async def advance_required(
        self,
        expected: int,
        target: int,
        expected_mod_revision: int,
        expected_services: Mapping[str, int],
        audited: Sequence[LiveCapability],
        activation_evidence_sha256: str,
        activation_evidence_completed_at_ms: int,
    ) -> None:
        """只允许 expected→target 的**前进** CAS,并写不可变审计记录。

        expected_mod_revision 必须来自锁内的 required_snapshot:
        值 + revision 双比较封住 1→2→1 的 ABA。
        """
        # Go: `if l == nil || !l.topologyLeaseVerified { return Err... }`
        # 没有任何代码路径会把它置 True —— 这条推进目前是**关闭**的,不是可重试故障。
        if not self._topology_lease_verified:
            raise _topology_lock_unavailable()
        self._ensure_held()
        _require_u32(expected, "expected epoch")
        _require_u32(target, "target epoch")
        _require_i64(expected_mod_revision, "expected mod revision")
        if expected == 0 or target <= expected or expected_mod_revision <= 0:
            raise PandoraError(
                ErrInvalidArg, "required epoch must advance: %d -> %d" % (expected, target)
            )
        _validate_activation_evidence_sha256(activation_evidence_sha256)
        _require_i64(activation_evidence_completed_at_ms, "activation evidence completion time")
        _validate_audited_target_policy(target, expected_services, audited)
        deps = _Deps()
        from_value = deps.required_value_for_epoch(expected)
        to_value = deps.required_value_for_epoch(target)
        now_ms = _now_unix_milli()
        if activation_evidence_completed_at_ms <= 0 or activation_evidence_completed_at_ms > now_ms:
            raise PandoraError(ErrInvalidState, "activation evidence completion time is invalid")
        key = deps.required_key(self._client.prefix)
        record_key = deps.clean_prefix(self._client.prefix) + "activations/" + str(target)
        record = ActivationRecord(
            from_=expected,
            to=target,
            from_required_value=from_value,
            to_required_value=to_value,
            required_policy_id=deps.policy_v2,
            from_mod_revision=expected_mod_revision,
            expected_services_hash=deps.expected_services_hash(dict(expected_services)),
            activation_evidence_sha256=activation_evidence_sha256,
            activation_evidence_completed_at_ms=activation_evidence_completed_at_ms,
            activated_at_ms=now_ms,
        )
        payload = record.to_json_bytes()
        compares = self._build_advance_compares(
            key, record_key, from_value, expected_mod_revision, audited
        )
        succeeded, _ = await self._client._txn(
            compares,
            [
                self._client._t.put(_b(key), _b(to_value)),
                self._client._t.put(_b(record_key), payload),
            ],
        )
        if not succeeded:
            raise PandoraError(
                ErrInvalidState,
                "required epoch CAS failed (expected=%d target=%d)" % (expected, target),
            )

    async def advance_required_policy_v3(
        self,
        expected: RequiredSnapshot,
        expected_services: Mapping[str, int],
        audited: Sequence[LiveCapability],
        activation_evidence_sha256: str,
        activation_evidence_completed_at_ms: int,
    ) -> None:
        """把不可变的 V2 原子推进到不可变的 V3,同时保留数据面 WriterEpoch=2。

        这是**只改策略**的迁移:需要 etcd 激活锁与精确的目标策略 capability 快照,
        但刻意**不**依赖数据面 writer-epoch 变更所需的 Redis 拓扑锁。
        """
        self._require_lock_present()
        self._ensure_held()
        deps = _Deps()
        if (
            expected.mod_revision <= 0
            or expected.policy_generation != deps.policy_generation_v2
            or expected.epoch != deps.protocol_epoch_v2
            or expected.policy_id != deps.policy_v2
            or expected.raw_value != deps.required_value_v2
        ):
            raise PandoraError(
                ErrInvalidArg, "required policy-only transition must advance from V2 to V3"
            )
        from_value = deps.required_value_for_policy_generation(expected.policy_generation)
        if expected.raw_value != from_value:
            raise PandoraError(ErrInvalidState, "required policy snapshot raw value mismatch")
        _validate_activation_evidence_sha256(activation_evidence_sha256)
        _require_i64(activation_evidence_completed_at_ms, "activation evidence completion time")
        _validate_audited_policy_generation(
            deps.policy_generation_v3, expected_services, audited
        )
        now_ms = _now_unix_milli()
        if activation_evidence_completed_at_ms <= 0 or activation_evidence_completed_at_ms > now_ms:
            raise PandoraError(ErrInvalidState, "activation evidence completion time is invalid")
        to_value = deps.required_value_v3
        record_key = _policy_activation_record_key(
            self._client.prefix, deps.policy_generation_v3, deps.policy_v3
        )
        record = ActivationRecord(
            from_=expected.epoch,
            to=deps.protocol_epoch_v2,
            from_policy_generation=expected.policy_generation,
            to_policy_generation=deps.policy_generation_v3,
            from_required_value=from_value,
            to_required_value=to_value,
            required_policy_id=deps.policy_v3,
            from_mod_revision=expected.mod_revision,
            expected_services_hash=deps.expected_services_hash(dict(expected_services)),
            activation_evidence_sha256=activation_evidence_sha256,
            activation_evidence_completed_at_ms=activation_evidence_completed_at_ms,
            activated_at_ms=now_ms,
        )
        payload = record.to_json_bytes()
        key = deps.required_key(self._client.prefix)
        compares = self._build_advance_compares(
            key, record_key, from_value, expected.mod_revision, audited
        )
        succeeded, _ = await self._client._txn(
            compares,
            [
                self._client._t.put(_b(key), _b(to_value)),
                self._client._t.put(_b(record_key), payload),
            ],
        )
        if not succeeded:
            raise PandoraError(
                ErrInvalidState,
                "required policy CAS failed (expected_generation=%d target_generation=%d)"
                % (expected.policy_generation, deps.policy_generation_v3),
            )

    async def advance_required_policy_v3_from_zero_writers(
        self,
        expected: RequiredSnapshot,
        activation_evidence_sha256: str,
        activation_evidence_completed_at_ms: int,
    ) -> None:
        """唯一受支持的 V1→V3 路径。

        acquire_lock 阻止新的 capability 注册;同一次改 required 的 CAS 还同时证明
        完整的 capability 前缀仍然为空。这是给全新 / 已重置、尚无任何 writer 进程
        启动的集群用的,**绝不能**换成手工 etcd put。
        """
        self._require_lock_present()
        self._ensure_held()
        deps = _Deps()
        if (
            expected.mod_revision <= 0
            or expected.policy_generation != deps.policy_generation_v1
            or expected.epoch != 1
            or expected.policy_id != ""
            or expected.raw_value != "1"
        ):
            raise PandoraError(
                ErrInvalidArg, "zero-writer policy bootstrap must advance exact V1 to V3"
            )
        _validate_activation_evidence_sha256(activation_evidence_sha256)
        _require_i64(activation_evidence_completed_at_ms, "activation evidence completion time")
        now_ms = _now_unix_milli()
        if activation_evidence_completed_at_ms <= 0 or activation_evidence_completed_at_ms > now_ms:
            raise PandoraError(ErrInvalidState, "activation evidence completion time is invalid")
        record_key = _policy_activation_record_key(
            self._client.prefix, deps.policy_generation_v3, deps.policy_v3
        )
        record = ActivationRecord(
            from_=1,
            to=deps.protocol_epoch_v2,
            from_policy_generation=deps.policy_generation_v1,
            to_policy_generation=deps.policy_generation_v3,
            from_required_value="1",
            to_required_value=deps.required_value_v3,
            required_policy_id=deps.policy_v3,
            from_mod_revision=expected.mod_revision,
            expected_services_hash=deps.expected_services_hash({}),
            activation_evidence_sha256=activation_evidence_sha256,
            activation_evidence_completed_at_ms=activation_evidence_completed_at_ms,
            activated_at_ms=now_ms,
            zero_writer_bootstrap=True,
        )
        payload = record.to_json_bytes()
        key = deps.required_key(self._client.prefix)
        compares = self._build_zero_writer_policy_advance_compares(
            key, record_key, expected.mod_revision
        )
        succeeded, _ = await self._client._txn(
            compares,
            [
                self._client._t.put(_b(key), _b(deps.required_value_v3)),
                self._client._t.put(_b(record_key), payload),
            ],
        )
        if not succeeded:
            raise PandoraError(
                ErrInvalidState,
                "zero-writer required policy CAS failed; V1 snapshot, lock, empty capability "
                "prefix, or create-only record changed",
            )

    async def bootstrap_required_policy_v3_from_missing(
        self,
        activation_evidence_sha256: str,
        activation_evidence_completed_at_ms: int,
        genesis_continuity_token: str,
    ) -> None:
        """崩溃安全的全新集群 genesis。

        不同于 missing→V1→V3 的两步命令,required V3 与它的不可变记录是**一起**创建的。
        激活锁加上 range 比较证明在线性化点上不存在任何 writer capability。
        """
        self._require_lock_present()
        self._ensure_held()
        _validate_activation_evidence_sha256(activation_evidence_sha256)
        validate_genesis_continuity_token(genesis_continuity_token)
        _require_i64(activation_evidence_completed_at_ms, "activation evidence completion time")
        now_ms = _now_unix_milli()
        if activation_evidence_completed_at_ms <= 0 or activation_evidence_completed_at_ms > now_ms:
            raise PandoraError(ErrInvalidState, "activation evidence completion time is invalid")
        deps = _Deps()
        record_key = _policy_activation_record_key(
            self._client.prefix, deps.policy_generation_v3, deps.policy_v3
        )
        record = ActivationRecord(
            from_=0,
            to=deps.protocol_epoch_v2,
            from_policy_generation=0,
            to_policy_generation=deps.policy_generation_v3,
            from_required_value="",
            to_required_value=deps.required_value_v3,
            required_policy_id=deps.policy_v3,
            from_mod_revision=0,
            expected_services_hash=deps.expected_services_hash({}),
            activation_evidence_sha256=activation_evidence_sha256,
            activation_evidence_completed_at_ms=activation_evidence_completed_at_ms,
            activated_at_ms=now_ms,
            zero_writer_bootstrap=True,
            genesis_bootstrap=True,
        )
        payload = record.to_json_bytes()
        key = deps.required_key(self._client.prefix)
        compares = self._build_missing_zero_writer_policy_bootstrap_compares(
            key, record_key, genesis_continuity_token
        )
        succeeded, _ = await self._client._txn(
            compares,
            [
                self._client._t.put(_b(key), _b(deps.required_value_v3)),
                self._client._t.put(_b(record_key), payload),
            ],
        )
        if not succeeded:
            raise PandoraError(
                ErrInvalidState,
                "fresh V3 genesis CAS failed; required/record/continuity, lock, or capability "
                "prefix changed",
            )

    # ── compare 构造 ────────────────────────────────────────────────────
    def _require_lock_present(self) -> None:
        # Go: `if l == nil || l.client == nil || l.token == "" { ... }`
        if self._client is None or self._token == "":
            raise PandoraError(ErrInvalidState, "activation lock is missing")

    def _build_advance_compares(
        self,
        key: str,
        record_key: str,
        expected_required_value: str,
        expected_mod_revision: int,
        audited: Sequence[LiveCapability],
    ) -> list[Any]:
        if (
            expected_required_value == ""
            or expected_mod_revision <= 0
            or key == ""
            or record_key == ""
            or self._token == ""
        ):
            raise PandoraError(ErrInvalidArg, "invalid required advance snapshot")
        t = self._client._t
        lock_key = _b(_dep("activation_lock_key")(self._client.prefix))
        compares = [
            t.value(_b(key)) == _b(expected_required_value),
            t.mod(_b(key)) == expected_mod_revision,
            t.create(_b(record_key)) == 0,
            t.value(lock_key) == _b(self._token),
        ]
        for capability in audited:
            if capability.key == "" or capability.mod_revision <= 0:
                raise PandoraError(ErrInvalidArg, "invalid audited capability revision")
            compares.append(t.mod(_b(capability.key)) == capability.mod_revision)
        return compares

    def _build_zero_writer_policy_advance_compares(
        self, key: str, record_key: str, expected_mod_revision: int
    ) -> list[Any]:
        if expected_mod_revision <= 0 or key == "" or record_key == "" or self._token == "":
            raise PandoraError(ErrInvalidArg, "invalid zero-writer policy advance snapshot")
        t = self._client._t
        prefix = self._client.prefix
        capabilities = _b(_dep("capability_prefix")(prefix))
        return [
            t.value(_b(key)) == b"1",
            t.mod(_b(key)) == expected_mod_revision,
            t.create(_b(record_key)) == 0,
            t.value(_b(_dep("activation_lock_key")(prefix))) == _b(self._token),
            t.create(capabilities, _prefix_range_end(capabilities)) == 0,
        ]

    def _build_missing_zero_writer_policy_bootstrap_compares(
        self, key: str, record_key: str, continuity_token: str
    ) -> list[Any]:
        if key == "" or record_key == "" or self._token == "":
            raise PandoraError(ErrInvalidArg, "invalid missing zero-writer V3 bootstrap snapshot")
        validate_genesis_continuity_token(continuity_token)
        t = self._client._t
        prefix = self._client.prefix
        capabilities = _b(_dep("capability_prefix")(prefix))
        continuity_key = _b(_genesis_continuity_key(prefix))
        authority_prefix = _b(_dep("clean_prefix")(prefix))
        authority_end = _prefix_range_end(authority_prefix)
        lock_key = _b(_dep("activation_lock_key")(prefix))
        # 两段 range 把「整个权威前缀里除了激活锁本身之外一个 key 都没有」拆成
        # [authority, lock) 与 (lock, authority_end) 两半 —— 这样锁自己不会
        # 把"前缀为空"的证明否掉。
        return [
            t.create(_b(key)) == 0,
            t.create(_b(record_key)) == 0,
            t.value(lock_key) == _b(self._token),
            t.create(authority_prefix, lock_key) == 0,
            t.create(lock_key + b"\x00", authority_end) == 0,
            t.version(continuity_key) == 1,
            t.value(continuity_key) == _b(continuity_token),
            t.create(capabilities, _prefix_range_end(capabilities)) == 0,
        ]


# ── 审计 ────────────────────────────────────────────────────────────────────
def audit_capabilities(
    capabilities: Sequence[LiveCapability], policy: AuditPolicy
) -> list[str]:
    """验证每个预期服务的实时副本数与不可变身份。

    返回 findings 列表(空 = 通过)。文案与排序与 Go 逐字节一致 ——
    findings 会进运维检索,漂移即检索失效。
    """
    deps = _Deps()
    counts: dict[str, int] = {}
    findings: list[str] = []
    seen_uid: set[str] = set()
    prefix = policy.prefix or deps.default_prefix
    target_writer_epoch = policy.target_epoch
    if policy.target_policy_generation != 0:
        try:
            target_writer_epoch = deps.required_writer_epoch_for_policy_generation(
                policy.target_policy_generation
            )
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001
            findings.append(_err_text(exc))

    for service, expected_features in policy.required_features.items():
        if service not in policy.required_services:
            findings.append("feature policy 包含未声明 writer %s" % (service,))
        for feature in expected_features:
            if _err_of(deps.validate_features, [feature]) is not None:
                findings.append("%s feature policy 非 canonical" % (service,))

    for service, digest in policy.expected_digests.items():
        if service not in policy.required_services:
            findings.append("digest policy 包含未声明 writer %s" % (service,))
        if not _digest_ok(digest):
            findings.append("%s expected image_digest 非 immutable digest" % (service,))

    for service in policy.required_services:
        if service not in policy.expected_digests:
            findings.append("%s 缺服务级 expected image_digest" % (service,))

    for live in capabilities:
        capability = live.capability
        if live.key != deps.capability_key(prefix, capability.service, capability.instance_uid):
            findings.append("capability key 与 payload 身份不一致: %s" % (live.key,))
        if live.lease_id == 0:
            findings.append("%s/%s 无 lease" % (capability.service, capability.instance_uid))
        if capability.writer_epoch != target_writer_epoch:
            findings.append(
                "%s/%s writer_epoch=%d != target=%d"
                % (
                    capability.service,
                    capability.instance_uid,
                    capability.writer_epoch,
                    target_writer_epoch,
                )
            )
        if policy.target_policy_generation == deps.policy_generation_v3 and (
            capability.supported_policy_generation != deps.policy_generation_v3
            or capability.supported_policy_id != deps.policy_v3
        ):
            findings.append(
                "%s/%s supported_policy=%d@%s != target=%d@%s"
                % (
                    capability.service,
                    capability.instance_uid,
                    capability.supported_policy_generation,
                    _go_quote(capability.supported_policy_id),
                    deps.policy_generation_v3,
                    _go_quote(deps.policy_v3),
                )
            )
        if policy.expected_acquired_policy_generation != 0:
            try:
                expected_policy_id = deps.required_policy_id_for_generation(
                    policy.expected_acquired_policy_generation
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:  # noqa: BLE001
                findings.append(_err_text(exc))
            else:
                if (
                    capability.acquired_policy_generation
                    != policy.expected_acquired_policy_generation
                    or capability.acquired_policy_id != expected_policy_id
                ):
                    findings.append(
                        "%s/%s acquired_policy=%d@%s != expected=%d@%s"
                        % (
                            capability.service,
                            capability.instance_uid,
                            capability.acquired_policy_generation,
                            _go_quote(capability.acquired_policy_id),
                            policy.expected_acquired_policy_generation,
                            _go_quote(expected_policy_id),
                        )
                    )
        if not _digest_ok(capability.image_digest):
            findings.append(
                "%s/%s image_digest 非 immutable digest"
                % (capability.service, capability.instance_uid)
            )
        if len(policy.allowed_digests) > 0 and capability.image_digest not in policy.allowed_digests:
            findings.append(
                "%s/%s image_digest 不在本次激活清单"
                % (capability.service, capability.instance_uid)
            )
        expected_digest = policy.expected_digests.get(capability.service)
        if expected_digest is None or capability.image_digest != expected_digest:
            findings.append(
                "%s/%s image_digest=%s, service expected=%s"
                % (
                    capability.service,
                    capability.instance_uid,
                    _go_quote(capability.image_digest),
                    _go_quote(expected_digest or ""),
                )
            )
        if policy.keyset_revision == "" or capability.keyset_revision != policy.keyset_revision:
            findings.append(
                "%s/%s keyset_revision=%s, expected=%s"
                % (
                    capability.service,
                    capability.instance_uid,
                    _go_quote(capability.keyset_revision),
                    _go_quote(policy.keyset_revision),
                )
            )
        if (
            policy.etcd_identity_revision != ""
            and capability.etcd_identity_revision != policy.etcd_identity_revision
        ):
            findings.append(
                "%s/%s etcd_identity_revision=%s, expected=%s"
                % (
                    capability.service,
                    capability.instance_uid,
                    _go_quote(capability.etcd_identity_revision),
                    _go_quote(policy.etcd_identity_revision),
                )
            )
        if _err_of(deps.validate_features, list(capability.features or [])) is not None:
            findings.append(
                "%s/%s capability features 非 canonical"
                % (capability.service, capability.instance_uid)
            )
        feature_set = set(capability.features or [])
        expected_features = policy.required_features.get(capability.service) or set()
        for feature in expected_features:
            if feature not in feature_set:
                findings.append(
                    "%s/%s 缺 capability feature=%s"
                    % (capability.service, capability.instance_uid, feature)
                )
        for feature in feature_set:
            if feature not in expected_features:
                findings.append(
                    "%s/%s 含未批准 capability feature=%s"
                    % (capability.service, capability.instance_uid, feature)
                )
        identity = capability.service + "/" + capability.instance_uid
        if identity in seen_uid:
            findings.append("重复 capability %s" % (identity,))
        seen_uid.add(identity)
        if len(policy.required_instances) > 0:
            instances = policy.required_instances.get(capability.service)
            if instances is None:
                findings.append(
                    "%s/%s 不在 K8s Pod UID 清单"
                    % (capability.service, capability.instance_uid)
                )
            elif capability.instance_uid not in instances:
                findings.append(
                    "%s/%s capability UID 不在 K8s 清单"
                    % (capability.service, capability.instance_uid)
                )
        counts[capability.service] = counts.get(capability.service, 0) + 1

    for service, want in policy.required_services.items():
        if want <= 0:
            findings.append("%s 预期副本数必须 >0" % (service,))
            continue
        if counts.get(service, 0) != want:
            findings.append(
                "%s capability=%d, expected=%d" % (service, counts.get(service, 0), want)
            )
    for service, count in counts.items():
        if service not in policy.required_services:
            findings.append("发现未在激活清单中的旧/额外 writer %s=%d" % (service, count))
    for service, instances in policy.required_instances.items():
        for uid in instances:
            if service + "/" + uid not in seen_uid:
                findings.append("K8s live Pod %s/%s 缺 capability lease" % (service, uid))
    # Go 的 sort.Strings 按 UTF-8 字节序;Python 按码点序 —— UTF-8 保序,两者等价。
    findings.sort()
    return findings


def _digest_ok(value: str) -> bool:
    """`digestPattern.MatchString(value)` 的等价判定。

    ★ 一律用 `fullmatch`。Python 的 `$` 会匹配「末尾换行之前」,
      `sha256:<64hex>\\n` 这种带尾换行的值用 `search`/`match` 会被判为合法 ——
      这正是硬性要求 6 要堵的洞。`fullmatch` 无论下游那条 pattern 怎么锚定都安全。
    """
    if not isinstance(value, str):
        return False
    return _dep("digest_pattern").fullmatch(value) is not None


# ── 解析器 ──────────────────────────────────────────────────────────────────
def parse_expected_digests(raw: str) -> dict[str, str]:
    """解析 `service=sha256:<64 lowercase hex>` 的精确服务级镜像清单。

    与全局 allowlist 不同,这个映射阻止一个 writer 冒用本次发布中**另一个服务**的
    合法 digest。
    """
    out: dict[str, str] = {}
    for item in raw.split(","):
        item = item.strip()
        if item == "":
            continue
        parts = item.split("=", 1)
        if (
            len(parts) != 2
            or parts[0] == ""
            or any(ch in parts[0] for ch in "/,=| ")
            or not _digest_ok(parts[1])
        ):
            raise PandoraError(
                ErrInvalidArg, "invalid expected service digest %s" % (_go_quote(item),)
            )
        if parts[0] in out:
            raise PandoraError(ErrInvalidArg, "duplicate digest service %s" % (_go_quote(parts[0]),))
        out[parts[0]] = parts[1]
    if not out:
        raise PandoraError(ErrInvalidArg, "expected service digests is empty")
    return out


def parse_required_features(raw: str) -> dict[str, set[str]]:
    """解析 `service=feature|feature,service=feature` 的精确必需能力。"""
    out: dict[str, set[str]] = {}
    if raw.strip() == "":
        return out
    validate_features = _Deps().validate_features
    for item in raw.split(","):
        parts = item.strip().split("=", 1)
        if len(parts) != 2 or parts[0] == "" or parts[1] == "":
            raise PandoraError(
                ErrInvalidArg, "invalid required features %s" % (_go_quote(item),)
            )
        if parts[0] in out:
            raise PandoraError(
                ErrInvalidArg, "duplicate feature service %s" % (_go_quote(parts[0]),)
            )
        feature_set: set[str] = set()
        for feature in parts[1].split("|"):
            if _err_of(validate_features, [feature]) is not None:
                raise PandoraError(ErrInvalidArg, "invalid feature for %s" % (parts[0],))
            if feature in feature_set:
                raise PandoraError(
                    ErrInvalidArg, "duplicate feature %s" % (_go_quote(feature),)
                )
            feature_set.add(feature)
        out[parts[0]] = feature_set
    return out


def parse_expected_services(raw: str) -> dict[str, int]:
    """解析 `service=count` 逗号清单。"""
    out: dict[str, int] = {}
    for item in raw.split(","):
        item = item.strip()
        if item == "":
            continue
        parts = item.split("=")
        if len(parts) != 2 or parts[0] == "":
            raise PandoraError(ErrInvalidArg, "invalid expected service %s" % (_go_quote(item),))
        count = _atoi(parts[1])
        if count is None or count <= 0:
            raise PandoraError(
                ErrInvalidArg, "invalid expected service count %s" % (_go_quote(item),)
            )
        if parts[0] in out:
            raise PandoraError(
                ErrInvalidArg, "duplicate expected service %s" % (_go_quote(parts[0]),)
            )
        out[parts[0]] = count
    if not out:
        raise PandoraError(ErrInvalidArg, "expected services is empty")
    return out


def _atoi(text: str) -> int | None:
    """`strconv.Atoi` 的等价物:只接受可选符号 + 十进制数字,并按 int64 判界。

    Python 的 `int()` 会吃掉空白、下划线分隔符(`1_0` → 10)与全角数字 —— 这三样
    Go 全都拒。清单是发布门禁的输入,宽松解析等于让一个手滑的参数悄悄变成别的数。
    """
    body = text[1:] if text[:1] in ("+", "-") else text
    if body == "" or not all("0" <= ch <= "9" for ch in body):
        return None
    value = int(text)
    if not (_I64_MIN <= value <= _I64_MAX):
        return None
    return value


def parse_expected_instances(raw: str) -> dict[str, set[str]]:
    """解析 `service=uid|uid,service=uid` 的精确 K8s Pod UID 清单。"""
    out: dict[str, set[str]] = {}
    for item in raw.split(","):
        item = item.strip()
        if item == "":
            continue
        parts = item.split("=", 1)
        if len(parts) != 2 or parts[0] == "" or parts[1] == "":
            raise PandoraError(
                ErrInvalidArg, "invalid expected instances %s" % (_go_quote(item),)
            )
        if parts[0] in out:
            raise PandoraError(
                ErrInvalidArg, "duplicate instance service %s" % (_go_quote(parts[0]),)
            )
        uid_set: set[str] = set()
        for uid in parts[1].split("|"):
            uid = uid.strip()
            if uid == "" or any(ch in uid for ch in "/,="):
                raise PandoraError(ErrInvalidArg, "invalid instance uid %s" % (_go_quote(uid),))
            if uid in uid_set:
                raise PandoraError(ErrInvalidArg, "duplicate instance uid %s" % (_go_quote(uid),))
            uid_set.add(uid)
        out[parts[0]] = uid_set
    if not out:
        raise PandoraError(ErrInvalidArg, "expected instances is empty")
    return out


# ── 目标策略校验 ────────────────────────────────────────────────────────────
def _validate_audited_target_policy(
    target: int, expected_services: Mapping[str, int], audited: Sequence[LiveCapability]
) -> None:
    deps = _Deps()
    if target != deps.protocol_epoch_v2:
        raise PandoraError(
            ErrInvalidArg, "unsupported audited target writer epoch %d" % (target,)
        )
    _validate_audited_policy_generation(deps.policy_generation_v2, expected_services, audited)


def _validate_audited_policy_generation(
    generation: int, expected_services: Mapping[str, int], audited: Sequence[LiveCapability]
) -> None:
    deps = _Deps()
    expected_policy = deps.required_features_for_policy_generation(generation)
    policy_features = {service: set(features) for service, features in expected_policy.items()}
    deps.validate_activation_policy_generation(generation, dict(expected_services), policy_features)
    target_value = deps.required_value_for_policy_generation(generation)
    state = deps.parse_required_state(_b(target_value))
    counts: dict[str, int] = {}
    target_writer_epoch = deps.required_writer_epoch_for_policy_generation(generation)
    for live in audited:
        capability = live.capability
        if capability.writer_epoch != target_writer_epoch:
            raise PandoraError(
                ErrInvalidState,
                "audited capability %s/%s writer epoch does not match target policy"
                % (capability.service, capability.instance_uid),
            )
        if generation == deps.policy_generation_v3 and (
            capability.supported_policy_generation != deps.policy_generation_v3
            or capability.supported_policy_id != deps.policy_v3
        ):
            raise PandoraError(
                ErrInvalidState,
                "audited capability %s/%s does not compile in exact target policy %d@%s"
                % (
                    capability.service,
                    capability.instance_uid,
                    deps.policy_generation_v3,
                    deps.policy_v3,
                ),
            )
        if generation == deps.policy_generation_v3 and (
            capability.acquired_policy_generation != deps.policy_generation_v2
            or capability.acquired_policy_id != deps.policy_v2
        ):
            raise PandoraError(
                ErrInvalidState,
                "audited staging capability %s/%s was not acquired against exact V2"
                % (capability.service, capability.instance_uid),
            )
        deps.validate_required_policy_for_capability(
            state, capability.service, capability.writer_epoch, list(capability.features or [])
        )
        counts[capability.service] = counts.get(capability.service, 0) + 1
    for service, expected in expected_services.items():
        if counts.get(service, 0) != expected:
            raise PandoraError(
                ErrInvalidState,
                "audited capability count for %s does not match activation policy" % (service,),
            )


# ── 记录 / 哨兵校验 ─────────────────────────────────────────────────────────
def _policy_activation_record_key(prefix: str, generation: int, policy_id: str) -> str:
    return _dep("clean_prefix")(prefix) + "activations/policies/" + str(generation) + "@" + policy_id


def _genesis_continuity_key(prefix: str) -> str:
    """哨兵刻意放在权威 DS-auth 前缀**之外**。

    这样 prepare / 重入才能证明"完整前缀为空",而 genesis CAS 才能证明
    激活锁是这个前缀里唯一先前存在的 key。
    """
    cleaned = _dep("clean_prefix")(prefix)
    trimmed = cleaned[:-1] if cleaned.endswith("/") else cleaned
    return trimmed + "-genesis-continuity"


def validate_genesis_continuity_token(token: str) -> None:
    """校验由不可变 K8s genesis 标记与 etcd 数据卷哨兵**双份**持有的随机 token。"""
    marker = "nonce:"
    if not isinstance(token, str) or not token.startswith(marker):
        raise PandoraError(
            ErrInvalidArg, "genesis continuity token must use nonce:<64 lowercase hex>"
        )
    raw_hex = token[len(marker) :]
    try:
        raw = bytes.fromhex(raw_hex)
    except (ValueError, binascii.Error) as exc:
        raise PandoraError(
            ErrInvalidArg, "genesis continuity token must use nonce:<64 lowercase hex>", cause=exc
        ) from exc
    # `bytes.fromhex` 接受大写与内嵌空白;Go 的 `hex.EncodeToString(raw) != rawHex`
    # 回环比较把这两类都挡掉。少了这一步,`NONCE` 的大写变体会被当成同一个 token,
    # 而 etcd 里存的是另一串字节 —— 哨兵比对会在别处离奇失败。
    if len(raw) != 32 or raw.hex() != raw_hex:
        raise PandoraError(
            ErrInvalidArg, "genesis continuity token must use nonce:<64 lowercase hex>"
        )


def _require_exact_create_only_sentinel(kv: _KV, token: str) -> None:
    if (
        kv.value != token
        or kv.version != 1
        or kv.create_revision <= 0
        or kv.mod_revision != kv.create_revision
    ):
        raise PandoraError(
            ErrInvalidState, "genesis continuity sentinel is not the exact create-only token"
        )


def _validate_zero_writer_services_topology(record: ActivationRecord) -> None:
    if record.zero_writer_bootstrap and record.expected_services_hash != _Deps().expected_services_hash({}):
        raise PandoraError(
            ErrInvalidState, "V3 zero-writer activation record has a non-empty services topology"
        )


def _validate_genesis_continuity_activation_provenance(
    record: ActivationRecord, continuity_create_revision: int, record_create_revision: int
) -> None:
    """阻止把 continuity 哨兵与一条不相干的 V1/V2 迁移记录凑成一对。

    本地 pending 标记只对 missing→V3 的直连 genesis 事务拥有哨兵,
    而那个事务必须**严格发生在**哨兵被持久化之后。
    """
    if (
        record.from_policy_generation != 0
        or not record.genesis_bootstrap
        or not record.zero_writer_bootstrap
        or record.from_ != 0
        or record.from_required_value != ""
        or record.from_mod_revision != 0
    ):
        raise PandoraError(
            ErrInvalidState, "V3 activation record is not the canonical continuity genesis"
        )
    if continuity_create_revision <= 0 or record_create_revision <= continuity_create_revision:
        raise PandoraError(
            ErrInvalidState, "V3 continuity sentinel was not created before the genesis record"
        )


def _is_canonical_lower_hex_sha256(value: str) -> bool:
    if not isinstance(value, str) or len(value) != 64 or value != value.lower():
        return False
    try:
        decoded = bytes.fromhex(value)
    except (ValueError, binascii.Error):
        return False
    return len(decoded) == 32


def _validate_activation_evidence_sha256(value: str) -> None:
    if not _digest_ok(value):
        raise PandoraError(ErrInvalidArg, "activation evidence must be immutable sha256 digest")


def validate_activation_evidence_input(
    current: int, target: int, digest: str, completed_at_ms: int, advancing: bool
) -> None:
    """让 legacy epoch-1 的只读审计保持可用,同时让 epoch 推进与**每一次已在目标上的
    审计**在两份外部证据缺一时 fail-closed。"""
    required = current == target or advancing
    if not required and digest == "" and completed_at_ms == 0:
        return
    _validate_activation_evidence_sha256(digest)
    if completed_at_ms <= 0:
        raise PandoraError(ErrInvalidArg, "activation evidence completion time is required")
    _require_i64(completed_at_ms, "activation evidence completion time")


def _topology_lock_unavailable() -> BaseException:
    """Go 的 ErrTopologyChangeLockProviderUnavailable —— 这是发布阻断,不是可重试的 etcd 故障。"""
    err = _dep("ErrTopologyChangeLockProviderUnavailable", "ERR_TOPOLOGY_CHANGE_LOCK_PROVIDER_UNAVAILABLE")
    if isinstance(err, BaseException):
        return err
    if isinstance(err, type) and issubclass(err, BaseException):
        return err()
    return PandoraError(ErrUnavailable, str(err))


def _capability_from_json(raw: bytes) -> Any:
    """把 capability JSON 解成 `dsauthfence.Capability`。

    解析权威在 `dsauthfence`(那边才有字段定义与 canonical 校验),这里只做转发。
    """
    fn = getattr(dsauthfence, "capability_from_json", None)
    if fn is not None:
        return fn(raw)
    cls = getattr(dsauthfence, "Capability", None)
    loader = getattr(cls, "from_json_bytes", None) if cls is not None else None
    if loader is not None:
        return loader(raw)
    raise PandoraError(
        ErrUnavailable,
        "dsauthfence 缺少 capability_from_json / Capability.from_json_bytes"
        "(见 dsauthfence_activate 的依赖契约块)",
    )


class _Deps:
    """把本模块用到的 `dsauthfence` 公开面收成一处,便于一眼看全依赖面。

    刻意做成**惰性属性**而不是 import 期常量:并行移植中的 `dsauthfence` 可能还没写完,
    import 期取值会让本模块整个不可导入,而不是在真正用到那个符号时才报缺。
    """

    __slots__ = ()

    @property
    def default_prefix(self) -> str:
        return _dep("DEFAULT_PREFIX")

    @property
    def protocol_epoch_v2(self) -> int:
        return int(_dep("PROTOCOL_EPOCH_V2"))

    @property
    def policy_v2(self) -> str:
        return _dep("REQUIRED_POLICY_V2")

    @property
    def policy_v3(self) -> str:
        return _dep("REQUIRED_POLICY_V3")

    @property
    def required_value_v2(self) -> str:
        return _dep("REQUIRED_VALUE_V2")

    @property
    def required_value_v3(self) -> str:
        return _dep("REQUIRED_VALUE_V3")

    @property
    def policy_generation_v1(self) -> int:
        return int(_dep("REQUIRED_POLICY_GENERATION_V1"))

    @property
    def policy_generation_v2(self) -> int:
        return int(_dep("REQUIRED_POLICY_GENERATION_V2"))

    @property
    def policy_generation_v3(self) -> int:
        return int(_dep("REQUIRED_POLICY_GENERATION_V3"))

    @property
    def clean_prefix(self) -> Callable[[str], str]:
        return _dep("clean_prefix")

    @property
    def required_key(self) -> Callable[[str], str]:
        return _dep("required_key")

    @property
    def capability_key(self) -> Callable[[str, str, str], str]:
        return _dep("capability_key")

    @property
    def validate_features(self) -> Callable[[Iterable[str]], Any]:
        return _dep("validate_features")

    @property
    def parse_required_state(self) -> Callable[[bytes], Any]:
        return _dep("parse_required_state")

    @property
    def required_value_for_epoch(self) -> Callable[[int], str]:
        return _dep("required_value_for_epoch")

    @property
    def required_value_for_policy_generation(self) -> Callable[[int], str]:
        return _dep("required_value_for_policy_generation")

    @property
    def required_policy_id_for_generation(self) -> Callable[[int], str]:
        return _dep("required_policy_id_for_generation")

    @property
    def required_writer_epoch_for_policy_generation(self) -> Callable[[int], int]:
        return _dep("required_writer_epoch_for_policy_generation")

    @property
    def required_features_for_policy_generation(self) -> Callable[[int], Mapping[str, Sequence[str]]]:
        return _dep("required_features_for_policy_generation")

    @property
    def validate_activation_policy_generation(self) -> Callable[..., Any]:
        return _dep("validate_activation_policy_generation")

    @property
    def validate_required_policy_for_capability(self) -> Callable[..., Any]:
        return _dep("validate_required_policy_for_capability")

    @property
    def expected_services_hash(self) -> Callable[[Mapping[str, int]], str]:
        return _dep("expected_services_hash")


# ── 依赖契约 ────────────────────────────────────────────────────────────────
#
# 本模块按下列签名调用 `pandorapy.dsauthfence`(Go 侧 fence.go / etcd.go /
# security.go 的移植)。命名一律是 **Go 同名符号的蛇形**;Go 侧不导出的符号
# (小写开头)在这里也按同名蛇形取,不加下划线前缀 —— 若移植方选择了带下划线的
# 私有名,请把它同时以无下划线名再导出一次,或直接改用下表的名字。
#
# 常量:
#   DEFAULT_PREFIX                          str    "/pandora/ds-auth/"
#   DEFAULT_DIAL_TIMEOUT                    float  秒(Go 是 5*time.Second)
#   PROTOCOL_EPOCH_V2                       int    2
#   REQUIRED_POLICY_V2 / REQUIRED_VALUE_V2  str
#   REQUIRED_POLICY_V3 / REQUIRED_VALUE_V3  str
#   REQUIRED_POLICY_GENERATION_V1/V2/V3     int    1 / 2 / 3
#   ErrTopologyChangeLockProviderUnavailable       异常实例或异常类
#                                           (备用名 ERR_TOPOLOGY_CHANGE_LOCK_PROVIDER_UNAVAILABLE)
#   digest_pattern                          re.Pattern,本模块只用 .fullmatch()
#
# key 构造(必须与 Go 逐字符一致,本模块**不**自己拼这些前缀):
#   clean_prefix(prefix) -> str                     Go cleanPrefix
#   required_key(prefix) -> str                     Go requiredKey
#   capability_prefix(prefix) -> str                Go capabilityPrefix
#   activation_lock_key(prefix) -> str              Go activationLockKey
#   capability_key(prefix, service, uid) -> str     Go capabilityKey
#
# 值 / 策略:
#   parse_required_state(raw: bytes) -> RequiredState
#       返回对象需有 .epoch / .policy_generation / .policy_id / .raw_value
#   required_value_for_epoch(epoch: int) -> str
#   required_value_for_policy_generation(gen: int) -> str
#   required_policy_id_for_generation(gen: int) -> str
#   required_writer_epoch_for_policy_generation(gen: int) -> int
#   required_features_for_policy_generation(gen: int) -> Mapping[str, Sequence[str]]
#   validate_features(features: Iterable[str]) -> None(失败抛异常;返回异常对象亦可)
#   validate_activation_policy_generation(gen, services: dict[str,int],
#                                         features: dict[str, set[str]]) -> None
#   validate_required_policy_for_capability(state, service, writer_epoch, features) -> None
#   expected_services_hash(services: Mapping[str,int]) -> str
#
# 类型 / 构造:
#   Capability          字段(蛇形):service / instance_uid / writer_epoch /
#                       supported_policy_generation / supported_policy_id /
#                       acquired_policy_generation / acquired_policy_id /
#                       image_digest / keyset_revision / etcd_identity_revision /
#                       started_at_ms / features
#   capability_from_json(raw: bytes) -> Capability
#       (备用:Capability.from_json_bytes(raw) 类方法)
#   ClientSecurity()    零值可构造
#   new_etcd_client(endpoints: list[str], timeout: float, prefix: str, security)
#       -> aetcd.Client(或其可 await 的构造协程)
#
# 「Go 返回 (value, error)」的函数在这里一律按 **抛异常** 使用;
# `_err_of` 同时兼容"返回异常对象"的直译风格,两种都能跑。
#
# 本模块**不依赖** fence.go 的 Holder / Acquire / AcquireRuntime / Backend /
# Lease / RequiredEvent / LostReason* —— 那些是业务进程侧的运行期栅栏,与激活工具无关。
