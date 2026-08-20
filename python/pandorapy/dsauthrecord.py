"""跨 DS 服务共享的授权记录 —— 对应 Go 侧 pkg/dsauthrecord/battle_result.go。

目前只有一种记录:BattleResultReceipt —— "battle_result 已经权威接收了这局结算" 的凭据。
它由 ds_allocator 在终态 CAS 里与 auth / battle 两个键**同一个事务**写入
(services/battle/ds_allocator/internal/data/battle_auth.go:1848),之后 ended 心跳
只能消费**完全匹配**的 receipt。

★ 双栈并行期为什么必须逐字段对齐 Go:
    这是一份落在 Redis 里的 JSON,Go 副本写、Python 副本读(或反过来)。
    字段名或类型对不上不会报错,只会解出一个各字段为零值的 receipt,
    于是 Valid() 返回 False → 心跳被判成"另一个 proof 的收据" → 终态释放被拒,
    表现为对局结束后 DS 迟迟不回收。所以键名、类型、序列化字节全部照抄。
"""

from __future__ import annotations

import dataclasses
import json

# 版本号。Go 侧是包内私有常量(battleResultReceiptVersion),这里导出是因为
# Python 没有包级私有 —— 但语义相同:只有 ==1 的记录才被 Valid() 认。
# 加字段时**不要**动这个值:滚动升级期新旧副本同时在线,改版本号等于让旧副本
# 把所有新写的 receipt 判成非法(§9.17 的双向兼容)。
BATTLE_RESULT_RECEIPT_VERSION = 1

_UINT32_MAX = (1 << 32) - 1
_UINT64_MAX = (1 << 64) - 1
_INT64_MIN = -(1 << 63)
_INT64_MAX = (1 << 63) - 1


def battle_result_receipt_key(match_id: int) -> str:
    """对应 Go 的 BattleResultReceiptKey。

    ★ 花括号不是装饰,是 Redis Cluster 的 hash tag:`{match_id}` 让本键与
      auth/battle 两个键落在**同一个 slot**,它们才能进同一个 MULTI/EXEC。
      去掉花括号后单机 Redis 上一切正常,上了 Cluster 才会以 CROSSSLOT 报错 ——
      典型的"开发环境测不出来"的坑。
    """
    if not isinstance(match_id, int) or match_id < 0 or match_id > _UINT64_MAX:
        raise ValueError(f"match_id {match_id!r} out of uint64 range")
    return f"pandora:ds:result-receipt:{{{match_id}}}"


@dataclasses.dataclass
class BattleResultReceipt:
    """battle_result 已权威接收结算的凭据。字段顺序 = Go struct 声明顺序 = JSON 序列化顺序。"""

    version: int = 0
    match_id: int = 0
    allocation_id: str = ""
    pod_name: str = ""
    instance_uid: str = ""
    instance_epoch: int = 0
    gen: int = 0
    jti: str = ""
    exp_ms: int = 0
    kid: str = ""
    token_sha256: str = ""
    writer_epoch: int = 0
    recorded_at_ms: int = 0

    def valid(self, now_ms: int) -> bool:
        """对应 Go 的 Valid。

        ★ 这里**故意不检查 exp_ms > now_ms** —— 方向与 Go 完全一致,不是漏写。
          receipt 是"已经发生过的事"的证明;签发它的那个 token 后来过期,
          并不能抹掉"结算已被权威接收"这个事实。加上过期检查会让一局对局在
          token TTL 之后永远无法完成终态释放(Go 的单测 battle_result_test.go
          最后一段专门断言 r.Valid(3000) 仍为 true)。

        时间上真正要挡的是另一头:recorded_at_ms 必须 > 0 且 ≤ now_ms
        (未来时间戳 = 时钟错乱或伪造),exp_ms 必须晚于 recorded_at_ms
        (签发即过期的凭据说明上游算错了 TTL)。
        """
        return (
            self.version == BATTLE_RESULT_RECEIPT_VERSION
            and self.match_id != 0
            and self.allocation_id != ""
            and self.pod_name != ""
            and self.instance_uid != ""
            and self.instance_epoch != 0
            and self.gen != 0
            and self.jti != ""
            and self.kid != ""
            and self.token_sha256 != ""
            and self.writer_epoch != 0
            and self.recorded_at_ms > 0
            and self.recorded_at_ms <= now_ms
            and self.exp_ms > self.recorded_at_ms
        )

    def same_credential(self, other: BattleResultReceipt) -> bool:
        """12 个凭据字段全等。对应 Go 的 SameCredential。

        ★ recorded_at_ms **不在**比较范围内,这是刻意的:immediate receipt 可能在
          DB commit 之后才写入,ds_allocator 会保留旧记录的真实 recorded_at
          (battle_auth.go:1829 `receipt.RecordedAtMs = old.RecordedAtMs`)。
          把它加进比较会让那条重入路径判成"属于另一个 proof",终态释放直接失败。
        """
        return (
            self.version == other.version
            and self.match_id == other.match_id
            and self.allocation_id == other.allocation_id
            and self.pod_name == other.pod_name
            and self.instance_uid == other.instance_uid
            and self.instance_epoch == other.instance_epoch
            and self.gen == other.gen
            and self.jti == other.jti
            and self.exp_ms == other.exp_ms
            and self.kid == other.kid
            and self.token_sha256 == other.token_sha256
            and self.writer_epoch == other.writer_epoch
        )


def new_battle_result_receipt(
    match_id: int,
    allocation_id: str,
    pod_name: str,
    instance_uid: str,
    instance_epoch: int,
    gen: int,
    jti: str,
    exp_ms: int,
    kid: str,
    token_sha256: str,
    writer_epoch: int,
    recorded_at_ms: int,
) -> BattleResultReceipt:
    """构造当前格式的 receipt。对应 Go 的 NewBattleResultReceipt。

    存在的唯一理由(照抄 Go 注释):避免各服务自己手填 version。
    参数顺序与 Go 逐个对齐,方便两边对照 review。
    """
    return BattleResultReceipt(
        version=BATTLE_RESULT_RECEIPT_VERSION,
        match_id=match_id,
        allocation_id=allocation_id,
        pod_name=pod_name,
        instance_uid=instance_uid,
        instance_epoch=instance_epoch,
        gen=gen,
        jti=jti,
        exp_ms=exp_ms,
        kid=kid,
        token_sha256=token_sha256,
        writer_epoch=writer_epoch,
        recorded_at_ms=recorded_at_ms,
    )


def marshal_battle_result_receipt(receipt: BattleResultReceipt) -> bytes:
    """序列化。对应 Go 的 MarshalBattleResultReceipt。

    ★ 先做类型域检查再做 Valid 检查:Go 那边 uint32/uint64/int64 的取值范围由
      **编译器**保证,函数体里只剩 Valid 一道。Python 的 int 无限精度,不补这一道
      就会写出一个 Go 的 json.Unmarshal 解不出来的数字(overflows int64),
      而写入方毫不知情 —— 双栈并行期就是"Python 写的 receipt Go 一条都读不了"。

    ★ Valid 用的 now 是 receipt.recorded_at_ms 本身(照抄 Go),所以
      `recorded_at_ms <= now_ms` 这条在这里恒真,实际起作用的是其余各条。
    """
    _check_ranges(receipt)
    if not receipt.valid(receipt.recorded_at_ms):
        raise ValueError("invalid battle result receipt")
    return _go_json_object(receipt).encode("utf-8")


def unmarshal_battle_result_receipt(payload: bytes | bytearray | str) -> BattleResultReceipt:
    """反序列化。对应 Go 的 UnmarshalBattleResultReceipt。

    ★ 刻意**不**在这里做 Valid 校验(Go 也没做):调用方拿到记录后要先和自己手上的
      凭据比 SameCredential,再决定用哪个 now 去 Valid。在这里提前判会丢掉那个选择。

    Go 的 json.Unmarshal 会因为类型不符 / 数值越界而报错,Python 的 json.loads
    什么都收 —— 差额必须在这里补齐,否则一条被截断的脏记录会被解成"各字段零值"
    的 receipt 并一路走下去。
    """
    if payload is None or len(payload) == 0:
        raise ValueError("empty battle result receipt")
    try:
        if isinstance(payload, (bytes, bytearray)):
            decoded = json.loads(bytes(payload).decode("utf-8"))
        else:
            decoded = json.loads(payload)
    except (ValueError, UnicodeDecodeError) as exc:
        raise ValueError(f"decode battle result receipt: {exc}") from exc

    receipt = BattleResultReceipt()
    if decoded is None:
        # Go: json.Unmarshal([]byte("null"), &v) 返回 nil error 且不动 v。
        # 这里照抄 —— 后续的 Valid() 会因为 version==0 拒掉它,不需要在这层报错。
        return receipt
    if not isinstance(decoded, dict):
        raise ValueError("decode battle result receipt: json: cannot unmarshal into struct")

    for key, value in decoded.items():
        field = _FIELD_BY_JSON_NAME.get(key)
        if field is None:
            # Go 的 encoding/json 在精确匹配失败后会做**大小写不敏感**的回退匹配。
            # 照抄这条(而不是直接忽略):忽略就意味着 Go 认得的记录 Python 读成零值。
            field = _FIELD_BY_FOLDED_NAME.get(key.lower())
        if field is None:
            continue  # 未知字段:Go 默认忽略
        name, kind = field
        if value is None:
            continue  # Go: JSON null 不改动目标字段
        setattr(receipt, name, _coerce(key, value, kind))
    return receipt


# ── 内部:类型域 ────────────────────────────────────────────────────────────────

# (python 属性名, JSON 键名, 数值域)。顺序 = Go struct 声明顺序 = 序列化字段顺序。
# Python 的 dict 保插入序,但"保插入序"不等于"按需要的顺序";这里显式列出来,
# 就不会有人靠 dataclasses.fields() 的巧合来决定 JSON 里字段的先后。
_FIELDS: tuple[tuple[str, str, str], ...] = (
    ("version", "version", "uint32"),
    ("match_id", "match_id", "uint64"),
    ("allocation_id", "allocation_id", "string"),
    ("pod_name", "pod_name", "string"),
    ("instance_uid", "instance_uid", "string"),
    ("instance_epoch", "instance_epoch", "uint32"),
    ("gen", "gen", "uint64"),
    ("jti", "jti", "string"),
    ("exp_ms", "exp_ms", "int64"),
    ("kid", "kid", "string"),
    ("token_sha256", "token_sha256", "string"),
    ("writer_epoch", "writer_epoch", "uint32"),
    ("recorded_at_ms", "recorded_at_ms", "int64"),
)

_FIELD_BY_JSON_NAME = {json_name: (name, kind) for name, json_name, kind in _FIELDS}
_FIELD_BY_FOLDED_NAME = {
    json_name.lower(): (name, kind) for name, json_name, kind in _FIELDS
}

_RANGES = {
    "uint32": (0, _UINT32_MAX),
    "uint64": (0, _UINT64_MAX),
    "int64": (_INT64_MIN, _INT64_MAX),
}


def _coerce(key: str, value: object, kind: str):
    if kind == "string":
        if not isinstance(value, str):
            raise ValueError(
                f"decode battle result receipt: json: cannot unmarshal into field {key} of type string"
            )
        return value
    # bool 必须在 int 之前判:Python 里 bool 是 int 的子类,不挡的话 JSON 的
    # true 会被静默当成 1 —— 而 Go 会明确报 "cannot unmarshal bool"。
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(
            f"decode battle result receipt: json: cannot unmarshal into field {key} of type {kind}"
        )
    low, high = _RANGES[kind]
    if value < low or value > high:
        raise ValueError(f"decode battle result receipt: number {value} overflows {kind}")
    return value


def _check_ranges(receipt: BattleResultReceipt) -> None:
    for name, _json_name, kind in _FIELDS:
        value = getattr(receipt, name)
        if kind == "string":
            if not isinstance(value, str):
                raise ValueError(f"battle result receipt field {name} must be str")
            continue
        low, high = _RANGES[kind]
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"battle result receipt field {name} must be int")
        if value < low or value > high:
            raise ValueError(f"battle result receipt field {name} out of {kind} range")


# ── 内部:与 Go encoding/json 逐字节一致的序列化 ────────────────────────────────
#
# 为什么不用 json.dumps 再打补丁,而是自己拼:两边的转义词表有两处不同,
# 而 dumps 之后再做字符串替换会误伤"用户数据里本来就有反斜杠"的情况
# (输入 "\\b" 被正确转义成 "\\\\b",再无脑替换 "\\b" 就把它改坏了)。
#   Go 转义 <  >  &  → \u003c \u003e \u0026(防 HTML 上下文注入),Python 不转义
#   Go 转义 U+2028/U+2029(JS 里是换行),Python 不转义
#   Python 默认 ensure_ascii=True 把所有非 ASCII 转成 \uXXXX,Go 原样输出 UTF-8
# 这些差异不影响双方**读**对方的数据(JSON 语义等价),但会让"按原始字节比对"的
# 手段失效。既然自己拼只要二十行,就没有理由留一个字节级不等价的实现。
#
# ★ 短转义(\b \f \n \r \t)与 Python 的 json 完全一致 —— 这一点是被
#   tests/test_dsauthrecord.py 的真 Go 输出**证伪一次之后**才定下的:
#   最初按"Go 只给 \n \r \t 短转义、\b \f 走 \u00xx"实现,对拍当场红。
#   凭记忆写 Go 的转义表是不靠谱的,以对拍结果为准。


def _go_json_string(value: str) -> str:
    out = ['"']
    for ch in value:
        cp = ord(ch)
        if ch == '"':
            out.append('\\"')
        elif ch == "\\":
            out.append("\\\\")
        elif ch == "\b":
            out.append("\\b")
        elif ch == "\f":
            out.append("\\f")
        elif ch == "\n":
            out.append("\\n")
        elif ch == "\r":
            out.append("\\r")
        elif ch == "\t":
            out.append("\\t")
        elif cp < 0x20 or ch in "<>&" or cp in (0x2028, 0x2029):
            out.append(f"\\u{cp:04x}")
        else:
            out.append(ch)
    out.append('"')
    return "".join(out)


def _go_json_object(receipt: BattleResultReceipt) -> str:
    parts = []
    for name, json_name, kind in _FIELDS:
        value = getattr(receipt, name)
        encoded = _go_json_string(value) if kind == "string" else str(value)
        parts.append(f'"{json_name}":{encoded}')
    return "{" + ",".join(parts) + "}"
