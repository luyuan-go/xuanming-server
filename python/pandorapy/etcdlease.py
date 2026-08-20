"""etcd lease 续约的唯一正确姿势 —— 三个选主/租约模块共用。

★ 为什么必须单独立一个件(2026-08-19,实测)

    Go 的 `clientv3.KeepAlive` 是一条**流**:租约没了,流就断/回 nil,调用方立刻知道。
    aetcd 没有自动续约,只有一次性的 `lease.refresh()`。而 etcd 对**已经不存在**的
    lease 的 LeaseKeepAlive 应答是:

        正常返回,不报错,只是把 TTL 置 0。

    本机真 etcd 实测(v3.5.17):

        put(key, lease=2s) → 睡 4s → key 已消失
        await lease.refresh()   # 不抛异常
        → LeaseKeepAliveResponse(ID=..., TTL=0)
        await lease.remaining_ttl() → -1

    于是「try: await refresh() except: 认为失败」这种写法有一个**永久静默**的洞:
    租约早就没了、key 早就被别人抢走了,而本副本因为"没抛异常"一直把本地安全
    截止线往后推 —— 它会**永远**认为自己还持有。三个模块原本都是这么写的:

        writerlease     → 两个副本同时对外宣告可写(fencing 形同虚设)
        etcdleader      → 两个副本同时跑撮合循环
        snowflake_etcd  → 两个副本用同一个 nodeID 发号(重号,不变量 §9.11)

    没有任何运行期信号会提醒你:日志照打续约成功,指标照样健康。

★ 关键区分:TTL<=0 是**失主的证据**,不是"这次没连上"

    续约抛异常(超时 / 连接断)只说明**没能证明**自己还持有 —— 那时按本地安全窗
    重试是对的,etcd 短抖动很常见。
    但 TTL<=0 是服务端明确告诉你「这个 lease 不存在」——**已经确定失主**,
    此时再等安全窗口只是在延长两个写者并存的时间。所以两者必须走不同分支:
    前者可重试,后者必须立即放弃。
"""

from __future__ import annotations

import asyncio


class LeaseGoneError(RuntimeError):
    """服务端明确回复该 lease 已不存在(TTL<=0)。**不可重试**,必须立即让位。"""


async def refresh_or_raise(lease, *, timeout: float) -> int:
    """续约一次;成功返回服务端给的新 TTL(秒)。

    - 连接层失败 → 原样抛(超时 / 网络异常),调用方按本地安全窗决定是否重试。
    - 服务端说 lease 没了 → 抛 `LeaseGoneError`,调用方必须**立即**放弃,不得重试。

    `refresh_lease` 在应答流为空时会隐式返回 None,同样按"lease 没了"处理 ——
    拿不到应答就等于证明不了自己还持有。
    """
    resp = await asyncio.wait_for(lease.refresh(), timeout=timeout)
    ttl = int(getattr(resp, "TTL", 0)) if resp is not None else 0
    if ttl <= 0:
        raise LeaseGoneError(
            f"etcd 回复 lease 已不存在(TTL={ttl});"
            f"独占权已丢失,继续持有会造成两个持有者并存"
        )
    return ttl
