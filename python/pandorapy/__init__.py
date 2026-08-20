"""Pandora 后端 Python 实现(strangler 迁移中,与 Go 版并存)。

包名刻意叫 pandorapy 而不是 pandora:proto 生成的 Python 模块用绝对导入且
第一级包名固定是 `pandora`(python/gen/pandora/...),重名会互相遮蔽。
"""
