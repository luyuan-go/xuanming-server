"""Go time.Duration 的字符串格式 —— 让日志里的时长字段与 Go 版逐字一致。

为什么需要:
    Go 侧 service_ready 打的是 `cfg.Dialogue.SessionTTL.Std().String()`,
    time.Duration.String() 对 5 分钟输出 **"5m0s"**,不是 yaml 里写的 "5m"。
    实测 Go 版真实输出:
        {"msg":"service_ready", ..., "session_ttl":"5m0s", ...}

    Python 侧若直接回显 yaml 原值("5m"),同一块面板上 Go 副本和 Python 副本的
    这个字段长得不一样。单看不致命,但灰度期正是要把两个实现的日志放在一起比对,
    字段值格式不同会让"两边配置是否一致"这个判断变得需要人脑换算。

Go 的规则(src/time/format.go Duration.String):
    - 0 → "0s"
    - 小于 1s 用 ns/µs/ms 单位
    - 否则按 h/m/s 拼接,秒位一定出现(所以 5 分钟是 "5m0s" 而非 "5m")
    - 小时存在时分钟位一定出现("1h0m0s")
"""

from __future__ import annotations

import datetime as _dt


def duration_string(td: _dt.timedelta) -> str:
    """把 timedelta 格式化成 Go time.Duration.String() 的形式。"""
    total_ns = round(td.total_seconds() * 1_000_000_000)
    if total_ns == 0:
        return "0s"

    sign = "-" if total_ns < 0 else ""
    ns = abs(total_ns)

    # 小于 1 秒:用最合适的小单位,与 Go 一致(会保留小数)。
    if ns < 1_000_000_000:
        if ns < 1_000:
            return f"{sign}{ns}ns"
        if ns < 1_000_000:
            return f"{sign}{_trim(ns / 1_000)}µs"
        return f"{sign}{_trim(ns / 1_000_000)}ms"

    seconds_total, rem_ns = divmod(ns, 1_000_000_000)
    hours, rem = divmod(seconds_total, 3600)
    minutes, seconds = divmod(rem, 60)

    sec_text = _trim(seconds + rem_ns / 1_000_000_000)
    if hours:
        # 有小时:分钟位与秒位都必须出现("1h0m0s")。
        return f"{sign}{hours}h{minutes}m{sec_text}s"
    if minutes:
        # 有分钟:秒位必须出现("5m0s")—— 这正是与 yaml 原值 "5m" 的差别所在。
        return f"{sign}{minutes}m{sec_text}s"
    return f"{sign}{sec_text}s"


def _trim(value: float) -> str:
    """去掉浮点尾零,整数就输出整数(Go 也不会打 "5.000")。

    ★ 不能用 `f"{value:g}"`:%g 只保留 **6 位有效数字**,而 Go 的
    time.Duration.String() 打的是完整精度(纳秒级)。1.0000005s 这类值
    在 %g 下会被截成 "1",于是同一个时长在两栈日志 / 配置回显里长得不一样,
    而这种差异不会报错 —— 只会让人以为两边配的不是同一个值。
    这里改成:定点展开到纳秒(Duration 的最小刻度)再去掉尾零。
    """
    if value == int(value):
        return str(int(value))
    text = f"{value:.9f}".rstrip("0").rstrip(".")
    return text or "0"
