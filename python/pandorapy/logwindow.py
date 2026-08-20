"""降级日志限流窗口 —— 对应 Go 侧 pkg/log/window.go 的 Window。

解决的问题(模式 C):
    弱依赖失败(Redis 抖动、kafka 挂)时,如果逐次打 Warn,会按 QPS 刷屏。
    但这条日志又往往是"降级正在发生"的**唯一信号** —— 业务 RPC 仍返回成功、
    access log 记 rpc_ok,删掉就彻底看不见了。

    所以既不能每次打,也不能不打。规则:
      - **首错必打**(降级开始的时刻要精确)
      - 窗口内只打一条,但带上累计失败次数(streak)
      - 恢复时打一条,界定降级区间

无 key 设计:
    刻意不按 player_id 之类的 key 分桶 —— 那些是高基数,分桶会让内存随玩家数
    无界增长。一个计数器 = 常量内存。代价是无法区分"哪个玩家的请求在降级",
    但降级本来就是依赖级故障,不是单玩家问题。

线程/协程安全:
    Go 用 atomic + CAS。Python 的 GIL 让 int 自增**不**原子(读-改-写三步),
    所以这里用一把 threading.Lock —— 这是纯内存操作,锁竞争可以忽略,
    而且它同时保护 asyncio 与线程池两条路径。
"""

from __future__ import annotations

import threading


class Window:
    """一个降级计数窗口。对应 Go 的 log.Window。"""

    __slots__ = ("_streak", "_extra", "_logged_ms", "_lock")

    def __init__(self) -> None:
        self._streak = 0  # 自上次成功以来的累计失败次数
        self._extra = 0  # 附加累计量(如累计丢弃帧数);不用时恒 0
        self._logged_ms = 0  # 上次实际打印时刻
        self._lock = threading.Lock()

    def admit(self, now_ms: int, window_ms: int) -> tuple[bool, int]:
        """记一次失败,返回 (本次是否应打印, 自上次成功以来的累计失败次数)。

        window_ms <= 0 时退化为"每次都打"(等价于不限流)。
        """
        with self._lock:
            self._streak += 1
            n = self._streak
            if n == 1:
                # 首错必打 —— 降级开始的时刻必须精确记录。
                self._logged_ms = now_ms
                return True, n
            if window_ms <= 0:
                return True, n
            if now_ms - self._logged_ms >= window_ms:
                self._logged_ms = now_ms
                return True, n
            return False, n

    def add_extra(self, n: int) -> int:
        """累加附加计量(如本次丢弃的帧数),返回累计值。"""
        with self._lock:
            self._extra += n
            return self._extra

    def recovered(self) -> tuple[int, int]:
        """在**成功路径**调用:归零并返回本轮累计失败次数与附加计量。

        failed > 0 表示此前处于降级状态,调用方应打一条"已恢复"日志 ——
        这条日志的作用是给降级区间画出右边界,否则只能看到"开始降级"看不到"恢复了"。
        """
        with self._lock:
            failed = self._streak
            self._streak = 0
            extra = 0
            if failed > 0:
                extra = self._extra
                self._extra = 0
            return failed, extra
