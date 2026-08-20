"""proto 枚举名格式化 —— 对齐 Go 生成代码的 `.String()` 语义。

## 为什么需要这个模块

Go 与 Python 的 protobuf 运行时在**未知枚举值**上行为相反,而 proto3 的枚举是
**开放**的(open enum):滚动升级期的旧副本、存储里的陈旧记录、上游新版本发来的
消息,都可以合法地携带一个本副本还不认识的数值。

实测(2026-08-19,真跑两侧):

    Go   locatorv1.LocationState(3).String()   -> "LOCATION_STATE_HUB"
    Go   locatorv1.LocationState(99).String()  -> "99"          ← 不 panic
    Go   locatorv1.LocationState(-1).String()  -> "-1"          ← 不 panic

    Py   locator_pb2.LocationState.Name(3)     -> "LOCATION_STATE_HUB"
    Py   locator_pb2.LocationState.Name(99)    -> ValueError!    ← 抛
    Py   locator_pb2.LocationState.Name(-1)    -> ValueError!    ← 抛

## 不用这个共享件会怎样

这 24 个调用点**全部在日志与错误消息的格式化路径上**,于是后果不是"日志少一行",
而是**格式化动作本身把整条业务路径炸掉**:

  ① 该 fail-closed 的地方拿不到 fail-closed。`hub_allocator` 的 presence 检查
     对未知状态要抛 `ErrUnavailable`(调用方据此退避重查),而构造那条错误消息时
     `.Name()` 先抛了 ValueError —— 调用方收到的是 gRPC `UNKNOWN`,分不清
     "该重试"还是"服务端有 bug",§9.23「不得无出口等待」当场打穿。

  ② 故障被归错因。栈顶是 `enum_type_wrapper.py:52 ValueError`,看起来像
     protobuf 库的问题,而真实原因是"上游发来了一个新枚举值"。

  ③ **只在混版窗口触发**。单测、单版本联调、压测全绿 —— 恰好是滚动升级
     (§9 不变量 21 要求的金丝雀发布)期间才炸,而那正是最不该出事的时候。

## 用法

    from pandorapy.protoenum import enum_name

    enum_name(locator_pb2.LocationState, state)   # 永不抛,语义同 Go .String()

★ 不要在新代码里直接写 `SomeEnum.Name(x)`。`tests/test_proto_enum_contract.py`
  按目录机械扫描,新增的裸 `.Name(` 会被判失败。
"""

from __future__ import annotations

from typing import Any

__all__ = ["enum_name"]


def enum_name(enum_type: Any, value: int) -> str:
    """返回枚举值的名字;未知值回落成十进制数字串(逐字对齐 Go 的 `.String()`)。

    Args:
        enum_type: 生成物里的 `EnumTypeWrapper`,如 `locator_pb2.LocationState`。
        value: 枚举数值。允许是本副本不认识的值(开放枚举)。

    Returns:
        已知值 -> proto 里声明的名字(如 ``"LOCATION_STATE_HUB"``);
        未知值 -> ``str(value)``(如 ``"99"`` / ``"-1"``),与 Go 一致。

    ★ 刻意**不**回落成 ``"UNKNOWN"`` 之类的占位串:那会把不同的未知值糊成同一个
      词,而排查混版问题时"具体是哪个数"正是唯一有用的信息。Go 保留了数字,
      两栈日志必须能对上(否则按枚举名建的 Loki 查询在两侧结果不同)。
    """
    try:
        return enum_type.Name(value)
    except (ValueError, TypeError):
        # ValueError = 该枚举没有这个数值(开放枚举的正常情况)。
        # TypeError  = 传进来的不是 int(例如上游把字段读成了 None);此时同样
        #              不该把日志路径炸掉,原样打出来让排查的人看见真实内容。
        return str(value)
