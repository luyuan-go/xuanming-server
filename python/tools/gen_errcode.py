"""从 Go 侧 pkg/errcode/errcode.go 生成 pandorapy/errcode.py。

为什么要生成而不是手写:
    错误码是跨语言契约 —— service 层做的是 `commonv1.ErrCode(errcode.As(err))`,
    即**按数值 1:1 映射到 proto enum**。Python 侧手抄 165 个码,只要有一个抄错或
    Go 侧新增时忘了同步,客户端就会收到错误的 code:业务失败被当成成功,或反过来。
    这类错误不会有编译错误也不会有运行异常,只会表现为客户端行为诡异。

    所以这里把 Go 源码当唯一真源,机械生成。配套的 tests/test_errcode_parity.py
    会在每次跑测试时重新解析 Go 源码比对,漂移当场变红。

用法:
    python tools/gen_errcode.py            # 生成
    python tools/gen_errcode.py --check    # 只校验是否与 Go 一致(CI 用),不写文件
"""

from __future__ import annotations

import argparse
import pathlib
import re
import sys

# 仓库根 = 本文件的上上级(python/tools/gen_errcode.py → XuanMing-Server/)
REPO = pathlib.Path(__file__).resolve().parents[2]

# 本工具的输出含中文,Windows 默认 cp1252 stdout 会抛 UnicodeEncodeError 并把退出码
# 污染成 1(文件其实已经写成功了)。必须在任何 print 之前强制 UTF-8。
sys.path.insert(0, str(REPO / "python"))
from pandorapy import _utf8  # noqa: E402,F401  (import 即生效)
GO_SRC = REPO / "pkg" / "errcode" / "errcode.go"
OUT = REPO / "python" / "pandorapy" / "errcode.py"

# 匹配 Go 的 `\tName Code = 123  // 注释`。限定行首一个 tab,避免误抓函数体里的赋值。
_CODE_RE = re.compile(
    r"^\t(?P<name>[A-Za-z][A-Za-z0-9]*)\s+Code\s*=\s*(?P<val>\d+)\s*(?://\s*(?P<comment>.*))?$",
    re.M,
)

HEADER = '''"""业务错误码 —— 由 tools/gen_errcode.py 从 pkg/errcode/errcode.go 生成,请勿手改。

重新生成:
    python tools/gen_errcode.py

契约:数值必须与 Go 侧、与 proto 的 pandora.common.v1.ErrCode **完全一致**。
service 层的映射是纯数值转换(`errcode_pb2.ErrCode.ValueType(code)`),
任何一个数值对不上都会让客户端收到错误的语义,且不报错。
"""

from __future__ import annotations

from typing import Final


class PandoraError(Exception):
    """带业务错误码的异常 —— 对应 Go 的 *errcode.Error。

    Go:
        errcode.New(errcode.ErrInvalidArg, "npc_id required")
    Python:
        raise PandoraError(ErrInvalidArg, "npc_id required")

    cause 对应 Go 的 NewCause:保留底层原因供上层判定(如 MySQL 1213 死锁重试),
    但**不改变** code 语义 —— 对客户端只暴露 code/msg,与 Go 侧一致。
    """

    __slots__ = ("code", "msg", "cause", "current_record", "retry_after_ms", "order_state")

    def __init__(self, code: int, msg: str = "", *args: object, cause: BaseException | None = None):
        # Go 侧 New(code, msg, args...) 用 fmt.Sprintf;这里对齐成 %% 风格格式化。
        self.msg = (msg % args) if args else msg
        self.code = code
        self.cause = cause
        # ★ 三个「随错误一起回给调用方的证据」。**通则**:凡 Go 侧写成
        # `(value, err)` 多返回、且失败路径上 value 仍然有意义的地方,Python 用异常
        # 传播就必须把那个 value 显式挂在异常上,否则**静默丢失** —— 调用方拿到零值,
        # 看起来只是"没有信息",实际是"信息被吞了",两者在日志里长得一模一样。
        #   current_record  epoch 冲突时的当前记录 —— §9.23 query-first 重查依据,
        #                   丢了调用方就得再打一次 Query,多一个 TOCTOU 窗口。
        #   retry_after_ms  屏障未开时的等待时长 —— §9.23 要求 WAIT 必须带明确
        #                   retry_after,丢了(=0)调用方要么空转要么干等。
        #   order_state     trade ConfirmOrder 失败时订单"现在停在哪"。Go 的
        #                   biz.ConfirmOrder 在**四条**失败路径上仍回真实状态
        #                   (EXPIRED / FAILED / SELLER_CONFIRMED ×2,见
        #                   services/economy/trade/internal/biz/trade.go:351/398/403/424),
        #                   service 层原样透传(service/trade.go:61-62)。丢了它,
        #                   客户端只看到 code=UNAVAILABLE + new_state=0,判不出该重试
        #                   还是该当订单没动 —— 而"已结算但终态未落库"必须重试。
        # 声明成 slots 而不是随手 setattr:后者能写进去(Exception 自带 __dict__)
        # 但拼写错一个字母不会报错,读的那侧只会永远拿到默认值。
        self.current_record: object | None = None
        self.retry_after_ms: int = 0
        self.order_state: int = 0
        super().__init__(f"errcode={code} {self.msg}")


def as_code(err: BaseException | None) -> int:
    """从异常提取错误码 —— 对应 Go 的 errcode.As(err)。

    语义必须逐条对齐 Go(pkg/errcode/errcode.go:359):
        err 为 None            → OK
        err 是 PandoraError     → 它的 code
        其它异常                → ErrUnknown
    还会沿 __cause__ / __context__ 回溯,对应 Go 的 errors.As 沿 Unwrap 链遍历。
    """
    if err is None:
        return OK
    if isinstance(err, PandoraError):
        return err.code
    seen: set[int] = set()
    cur: BaseException | None = err
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        if isinstance(cur, PandoraError):
            return cur.code
        cur = cur.__cause__ or cur.__context__
    return ErrUnknown


# ── 以下由生成器写入 ──────────────────────────────────────────────────────────
'''


def parse_go_codes(go_source: str) -> list[tuple[str, int, str]]:
    """解析 Go 源码,返回 [(名字, 数值, 注释), ...],保持源码顺序。"""
    return [
        (m.group("name"), int(m.group("val")), (m.group("comment") or "").strip())
        for m in _CODE_RE.finditer(go_source)
    ]


def go_name_to_py(name: str) -> str:
    """Go 的导出名直接沿用,不转 snake_case。

    刻意保持 `ErrDialogueNotFound` 而不是 `ERR_DIALOGUE_NOT_FOUND`:
    迁移期间两边代码会被反复对照阅读,同名能让 diff 一眼可比。PEP8 的常量大写
    在这里让位于跨语言可比性(与 proto enum 名也不同,proto 那套是 ERR_XXX)。
    """
    return name


def render(codes: list[tuple[str, int, str]]) -> str:
    lines = [HEADER]
    for name, value, comment in codes:
        py = go_name_to_py(name)
        if comment:
            lines.append(f"# {comment}")
        lines.append(f"{py}: Final[int] = {value}")
    lines.append("")
    lines.append("# 名字 → 数值,供 parity 测试与调试反查使用。")
    lines.append("ALL_CODES: Final[dict[str, int]] = {")
    for name, value, _ in codes:
        lines.append(f'    "{go_name_to_py(name)}": {value},')
    lines.append("}")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--check", action="store_true", help="只校验不写文件(CI 用)")
    args = ap.parse_args()

    if not GO_SRC.exists():
        print(f"[ERR] 找不到 Go 源码: {GO_SRC}", file=sys.stderr)
        return 1

    codes = parse_go_codes(GO_SRC.read_text(encoding="utf-8"))
    if not codes:
        print(f"[ERR] 未从 {GO_SRC} 解析到任何错误码 —— 正则与源码格式已不匹配", file=sys.stderr)
        return 1

    # 数值重复 = Go 侧写错了,这里当场拒绝而不是生成一个后写覆盖前写的模块。
    seen: dict[int, str] = {}
    for name, value, _ in codes:
        if value in seen:
            print(f"[ERR] 错误码数值重复: {value} 同时是 {seen[value]} 和 {name}", file=sys.stderr)
            return 1
        seen[value] = name

    content = render(codes)

    if args.check:
        if not OUT.exists():
            print(f"[ERR] {OUT} 不存在,请先运行 python tools/gen_errcode.py", file=sys.stderr)
            return 1
        if OUT.read_text(encoding="utf-8") != content:
            print(
                f"[ERR] {OUT.name} 与 Go 侧 errcode.go 不一致 —— 请重新运行 "
                f"python tools/gen_errcode.py 并提交",
                file=sys.stderr,
            )
            return 1
        print(f"[OK ] errcode 与 Go 侧一致({len(codes)} 个码)")
        return 0

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(content, encoding="utf-8", newline="\n")
    print(f"[OK ] 已生成 {OUT.relative_to(REPO)}({len(codes)} 个错误码)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
