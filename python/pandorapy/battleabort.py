"""Matchmaker → DS allocator 的"准入前分配中止"签名体 —— 对应 Go 侧 pkg/battleabort/abort.go。

这条 RPC 会在 DS 还没接客之前把一次已经开始的 battle 分配作废。它必须被签名,
因为能伪造它就等于能任意拆掉别人正在进行的分配。本模块只做两件事:

  1. **canonical()**:把请求编码成待签字节。编码必须与 Go 逐字节相同 ——
     Go 侧签名、Python 侧验签(或反过来)时,少一个字节就是全部验签失败。
  2. **complete() / valid_target()**:签名**之前**的形状闸。

★ 为什么每个变长字段都带 4 字节长度前缀,而不是用换行/逗号拼接(照抄 Go 头注释):
    分隔符拼接的编码不是单射的 —— pod_name="a\\nb", uid="c" 与 pod_name="a", uid="b\\nc"
    会拼出同一个待签串,于是一个针对 A 实例的合法签名可以被重放成针对 B 实例的中止。
    长度前缀让字段边界成为签名内容的一部分,边界一移动字节就变了。
    tests/test_battleabort.py 有一条专门的碰撞用例守着这个性质。
"""

from __future__ import annotations

import dataclasses
import struct

from pandorapy import releasetrack
from pandorapy.placement import (
    Target,
    go_is_control,
    go_is_space,
    go_trim_space,
    valid_operation_id,
)

# 域分隔串。它让本编码产出的字节永远不会与其它签名体撞上 —— 同一把密钥签的
# "battle 中止"不能被拿去冒充别的用途。带 -v1 后缀是为了将来换编码时能并存。
CANONICAL_DOMAIN = "pandora-battle-allocation-abort-v1"

# 字段字节上限。253 是 DNS label 全长上限(k8s Pod 名的硬边界);128 是 UID /
# allocation ID 的宽松上限。注意 Go 的 len(string) 是**字节数**不是字符数,
# Python 的 len(str) 是字符数 —— 非 ASCII 字段上这两者会差好几倍,必须先 encode。
MAX_POD_NAME_BYTES = 253
MAX_INSTANCE_UID_BYTES = 128
MAX_ALLOCATION_ID_BYTES = 128

_UINT32_MAX = (1 << 32) - 1
_UINT64_MAX = (1 << 64) - 1


def valid_target(target: Target) -> bool:
    """签名体与 allocator 持久化拆除凭据**共用**的身份闸。对应 Go 的 ValidTarget。

    ★ 为什么必须是同一个校验器(照抄 Go 注释的理由):
        如果拆除标记的形状闸比签名体宽松,就能造出一个"任何合法签名都命名不到"的标记,
        再把它当成终态权威。两边共用一个函数,那种标记根本构造不出来。

    assignment_id 必须为空:这是 **Battle** 分配的中止,带上 Hub 的 assignment_id
    说明调用方拿错了 target 类型,继续走下去会去拆一个 Hub 座位。
    """
    return (
        target.complete_battle()
        and target.assignment_id == ""
        and _valid_canonical_field(target.pod_name, MAX_POD_NAME_BYTES)
        and _valid_canonical_field(target.instance_uid, MAX_INSTANCE_UID_BYTES)
        and _valid_canonical_field(target.allocation_id, MAX_ALLOCATION_ID_BYTES)
        and (
            target.release_track == releasetrack.STABLE
            or target.release_track == releasetrack.CANARY
        )
    )


@dataclasses.dataclass(frozen=True)
class Request:
    """绑定到已鉴权的 abort RPC 上的请求体。对应 Go 的 battleabort.Request。"""

    match_id: int = 0
    operation_id: str = ""
    target: Target = dataclasses.field(default_factory=Target)

    def complete(self) -> bool:
        """签名前的形状闸。对应 Go 的 Request.Complete。

        operation_id 走 §9.23 的 canonical UUIDv4 校验(placement.valid_operation_id):
        它是这次中止的幂等键,写法不归一就会被当成两次不同的 operation。
        """
        return (
            isinstance(self.match_id, int)
            and 0 < self.match_id <= _UINT64_MAX
            and valid_operation_id(self.operation_id)
            and valid_target(self.target)
        )

    def canonical(self) -> bytes:
        """产出待签字节。对应 Go 的 Request.Canonical。

        ★ 刻意**不**在这里调用 complete():Go 那边也没调。canonical() 必须对任意
          输入都能算出字节,否则 tests 里"畸形字段是否改变签名体"的碰撞用例就没法验。
          形状闸是调用方在签名前的责任,不是编码器的。

        字段顺序即协议,不能重排:domain, match_id, operation_id, pod_name,
        instance_uid, instance_epoch, allocation_id, release_track。
        (注意 assignment_id **不进**编码 —— valid_target 已经强制它为空。)
        """
        out = bytearray()
        _write_canonical_string(out, CANONICAL_DOMAIN)
        out += _pack_uint64(self.match_id, "match_id")
        _write_canonical_string(out, self.operation_id)
        _write_canonical_string(out, self.target.pod_name)
        _write_canonical_string(out, self.target.instance_uid)
        out += _pack_uint32(self.target.instance_epoch, "instance_epoch")
        _write_canonical_string(out, self.target.allocation_id)
        _write_canonical_string(out, self.target.release_track)
        return bytes(out)


def _write_canonical_string(out: bytearray, value: str) -> None:
    """4 字节大端长度前缀(字节数)+ UTF-8 原文。对应 Go 的 writeCanonicalString。"""
    raw = value.encode("utf-8")
    out += _pack_uint32(len(raw), "canonical field length")
    out += raw


def _pack_uint64(value: int, name: str) -> bytes:
    if not isinstance(value, int) or value < 0 or value > _UINT64_MAX:
        raise ValueError(f"{name} {value!r} out of uint64 range")
    return struct.pack(">Q", value)


def _pack_uint32(value: int, name: str) -> bytes:
    """越界直接抛,**不**模 2**32 回绕。

    Go 的 `binary.Write(out, BigEndian, uint32(len(value)))` 在超过 4 GiB 时是静默截断的;
    Python 的 int 没有这个上界,照抄"回绕"等于主动制造一个签名体碰撞
    (长度 L 与 L+2**32 编码相同)。这里选 fail-closed:真实字段早被 253/128 字节闸挡住,
    走到这一步就说明调用方传了个 Go 里不可能存在的值,应该炸而不是签出一个歧义的体。
    """
    if not isinstance(value, int) or value < 0 or value > _UINT32_MAX:
        raise ValueError(f"{name} {value!r} out of uint32 range")
    return struct.pack(">I", value)


def _valid_canonical_field(value: str, max_bytes: int) -> bool:
    """对应 Go 的 validCanonicalField:非空、无首尾空白、字节数不超、无控制符/空白符。

    四条判据里 "无首尾空白" 严格来说被 "整串无空白" 覆盖(Go 也是这样冗余写的),
    照抄是为了让两边的语句能逐条对上,将来 Go 那边放宽其中一条时 diff 一眼可见。

    为什么整串禁空白与控制符:这些字段会进 annotation、日志与 k8s 对象名。
    一个藏在中间的 \\n 能把单行日志劈成两行、把一个 annotation 伪装成两个键值对。
    """
    if not isinstance(value, str) or value == "":
        return False
    if value != go_trim_space(value):
        return False
    if len(value.encode("utf-8")) > max_bytes:
        return False
    for ch in value:
        if go_is_control(ch) or go_is_space(ch):
            return False
    return True
