"""强制进程 I/O 走 UTF-8 —— 每个入口(服务 main / 工具脚本 / 测试)都必须先 import 它。

为什么需要这个模块(实测 2026-08-18):
    CPython 在 Windows 上,stdout/stderr 的默认编码取自系统 ANSI 代码页(本机是 cp1252,
    中文环境常见的是 cp936/gbk),**不是 UTF-8**。于是:

        print("已生成 errcode.py")
        → UnicodeEncodeError: 'charmap' codec can't encode characters ...

    这在本仓库不是边角情况,而是必然发生:
      - 日志字段值大量是中文(NPC 说话人"商店老板"、配表名"对话/d_对话.xlsx")
      - 错误消息、启动横幅、工具输出都是中文
    Go 的 zap 直接写 UTF-8 字节,从来没有这一层,所以迁移时极易漏掉。

    更糟的是失败形态:日志被整条丢弃并抛异常。如果它发生在一个 except 分支里,
    就会把真正的错误盖掉,变成"报了个编码错,原始故障看不见"。

两道保险:
    1. reconfigure —— 对已经建好的 stdout/stderr 生效(本进程内立即可用)
    2. errors="backslashreplace" —— 万一仍遇到不可编码字符,转义而不是丢日志/抛异常

另外建议在部署侧设 PYTHONUTF8=1(Python UTF-8 Mode),它覆盖面更广(含文件默认编码)。
本模块不依赖那个环境变量,是为了让直接 `python xxx.py` 也不会踩坑。
"""

from __future__ import annotations

import sys


def force_utf8() -> None:
    """把 stdout/stderr 重新配置成 UTF-8。可重复调用,幂等。"""
    for stream in (sys.stdout, sys.stderr):
        # reconfigure 只在 TextIOWrapper 上存在;被重定向到自定义对象时跳过即可。
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is None:
            continue
        try:
            reconfigure(encoding="utf-8", errors="backslashreplace")
        except (ValueError, OSError):
            # 流已关闭或不支持重配置(如 pytest 的 capture)——不是致命问题,忽略。
            pass


# import 即生效:调用方只需 `from pandorapy import _utf8`(配 F401 抑制注释)
force_utf8()
