"""etcd 测试的共享门控件(2026-08-19 建)。

★ 为什么要有这个文件

    test_etcdleader / test_snowflake_etcd / test_writerlease 各写了一份**一模一样**的
    探针:

        async def _etcd_available() -> bool:
            try:
                ... aetcd.Client(...).get(...)
                return True
            except Exception:
                return False

    两个问题叠在一起,合起来能让 44 条真 etcd 用例静默消失而退出码 0:

      ① `except Exception -> return False` 把**代码类异常整体折叠成一个布尔**。
         零 mock 的真实触发路径:`PANDORA_TEST_ETCD_ENDPOINTS` 漏写端口
         (写成 `127.0.0.1` 而不是 `127.0.0.1:12379`)→ `int(port)` 抛 ValueError
         → 44 条全跳过。而 tools/scripts/ci_db.ps1 正是手拼这个字符串的。
      ② 随后的 `pytest.skip(...)` 文案里**一个异常字段都没有**,只有
         "etcd 不可用 @ 127.0.0.1:12379" 加一条 `docker run` 建议 —— 容器明明跑着。
         这不是"日志里没线索",是**线索指错了方向**:读日志的人去查 etcd,
         查不出问题,然后放过。误导性文案比没文案更贵。

    丢掉的覆盖正是选主 / writer lease fencing / snowflake nodeID 抢占,
    也就是 §9.21 / §9.22 的分布式权威面;而 tools/scripts/ci_backend.ps1:311 的
    skip 审计只按 `MySQL|TiDB` 升级为失败,**etcd 这 44 条在任何 flag 下都只是 WARN**。

★ Go 那边为什么不长这样

    全仓唯一打真 etcd 的 Go 用例
    `services/social/mission/internal/biz/push_writer_lease_smoke_test.go:25-27`
    的闸门是**环境变量是否存在**(`PANDORA_TEST_ETCD_ENDPOINTS == "" → t.Skip`);
    变量一旦设了,`writerlease.Start()` 出错走的是 `t.Fatalf` —— 驱动 / 连接类故障
    在 Go 侧是**红的,不是跳过的**。Go 侧压根没有"先探活再吞异常"这个形状。

    本套自己也有正确写法:`tests/test_login_main.py` 的 MySQL 探针一直带 `{exc}`。
    所以 etcd 这三处不是项目惯例,是对本套自有惯例的偏离。

★ 现在的判据

    只有**连接类**异常才 skip,其余(ValueError / AttributeError / TypeError …)
    原样抛出去变红;skip 文案必须带上真异常。
"""

from __future__ import annotations

import asyncio
import os

# 与 Go 侧 CI 的门控变量同名。多端点时只用第一个(测试只需要一个可写节点)。
ENDPOINT = os.getenv("PANDORA_TEST_ETCD_ENDPOINTS", "127.0.0.1:12379").split(",")[0]

_DOCKER_HINT = (
    "起一个:docker run -d -p 12379:2379 quay.io/coreos/etcd:v3.5.17 etcd "
    "--listen-client-urls http://0.0.0.0:2379 "
    "--advertise-client-urls http://127.0.0.1:12379"
)


def host_port(endpoint: str = "") -> tuple[str, int]:
    """拆 `host:port`。**端点写错必须炸,不能变成"etcd 不可用"**。

    原写法是裸 `int(port)`:端点漏写端口时抛 ValueError,而调用它的探针
    `except Exception: return False` 把这条配置错误洗成了"环境不可用",
    44 条用例跳过、退出码 0、文案还让你去起一个已经在跑的容器。
    """
    ep = endpoint or ENDPOINT
    host, _, port = ep.rpartition(":")
    if not port.isdigit():
        raise ValueError(
            f"PANDORA_TEST_ETCD_ENDPOINTS 不是 host:port 形状:{ep!r} —— "
            f"这是配置写错,不是 etcd 不可用"
        )
    return host or "127.0.0.1", int(port)


def _env_failure_types() -> tuple[type[BaseException], ...]:
    """"etcd 没起来"的异常放行集 —— 只有这几类才允许 skip。

    实测(2026-08-19,aetcd 1.x):端口没人听 → `aetcd.exceptions.ConnectionTimeoutError`。
    刻意**不**收基类 `ClientError`:它同时是 `InvalidArgumentError` /
    `PreconditionFailedError` 的父类,而那两类是"我们把请求发错了",必须冒红。
    """
    types: list[type[BaseException]] = [TimeoutError, OSError]
    try:
        import aetcd.exceptions as _ax
    except Exception:  # noqa: BLE001  —— 没装 aetcd 时被测模块的顶层 import 早已炸了
        return tuple(types)
    types.extend([_ax.ConnectionFailedError, _ax.ConnectionTimeoutError])
    return tuple(types)


async def require_etcd(what: str) -> None:
    """探活;连不上就 skip(文案带真异常),**探针自己出错一律冒红**。"""
    import pytest

    import aetcd

    host, port = host_port()  # 端点写错 → ValueError 冒红,不洗成"不可用"
    try:
        async with aetcd.Client(host=host, port=port, timeout=2) as client:
            await asyncio.wait_for(client.get(b"/pandora/probe"), timeout=3)
    except _env_failure_types() as exc:
        pytest.skip(
            f"etcd 不可用 @ {ENDPOINT}({exc!r}) —— {what}整体跳过(不假装通过)。{_DOCKER_HINT}"
        )
